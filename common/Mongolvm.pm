# Description:  MongoDB LVM备份工具

package Mongolvm;

use strict;
use warnings;

use DBI;
use POSIX qw(strftime);
use POSIX qw(SIGALRM);
use POSIX ":sys_wait_h";
use File::Path;
use Log::Log4perl;

$SIG{CHLD} = 'IGNORE';


# 构造函数
sub new {
    my ( $class, %args ) = @_;
    
    my $self = {};  # create a hash ref
    
    my $log = Log::Log4perl->get_logger(""); 

    # 接收$dbconfigObj和$dbbackupObj对象
    for (qw(dbconfigObj dbbackupObj)) {
        if ( $args{$_} ) {
            $self->{$_} = $args{$_};
        } else {
            $log->error("got no $_");
            die "got no $_!";
        }
    }
    
    bless ( $self, $class );
    
    return $self;
}

# @Description:  执行备份 
# @Param: 
# @Return:  1:success  0:failed
sub main {
    my ( $self,$host,$port,$dbType,$isSlave,$bakType,$level,$levelValue,$backupDate,
        $isEncrypted,$isCompressed,$retention,$storageType,
        $lvmExpireDays,$lvmSpeed ) = @_;        

    my $log = Log::Log4perl->get_logger("");

    $log->info("starting lvm backup"); 

    # 实例备份目录
    my $baseDir = $self->{dbbackupObj}->getStorageDir($host,$port,$dbType,$storageType);
    if ( ! $baseDir ){
        $log->error("storage dir is not mounted, stop backup");
        return 0;
    }
    $log->debug("get \$baseDir: $baseDir");
    
    my $fileDir = "lvm/$backupDate\_$host\_$port";
    $log->debug("get \$fileDir: $fileDir");
    
    my $bakDir = "$baseDir/$fileDir";
    $log->debug("get \$bakDir: $bakDir");
        
    my $forceDelete = 1;
    my $flushTableLockLimit = 300;

    # 创建lvm备份存储目录
    if ( ! -d "$baseDir/lvm" ){
        mkdir("$baseDir/lvm");
        $log->info("create dir: $baseDir/lvm success");
    }

    # 日志目录
    if ( ! -d "/home/mongodb/dbadmin/logs/$backupDate" ){
        mkdir("/home/mongodb/dbadmin/logs/$backupDate");
    }

    # 日志文件
    my $logPrefix = "/home/mongodb/dbadmin/logs/$backupDate/$backupDate\_$host\_$port\_$bakType";
    my $mainLog = "$logPrefix.log";
    my $detailLog = "$logPrefix.detail";
    my $traceLog = "$logPrefix.trace";

    # 获取filer存储目录
    my $filerDir = $self->{dbbackupObj}->getFilerDir($host,$port,$dbType,$storageType);
    $log->info("get filerDir: $filerDir");

    # 设置默认db
    my $defaultDb = "admin";
    
    # 获取实例数据目录
    my $dataDir = $self->{dbbackupObj}->getMongoInstanceDataDir($port);
    $log->debug("get \$dataDir: $dataDir");
    
    if ( !defined($dataDir) ){
        $log->error("get dataDir failed");
    }

    # 检查备份
    my $isExistBackupset = $self->{dbbackupObj}->isExistBackupset($host,$port,$dbType,$bakType,$bakDir,$fileDir);
    chomp($isExistBackupset);
    $log->debug("get \$isExistBackupset: $isExistBackupset");

    if ( $isExistBackupset == 0 ){

        # 更新备份资料库
        my $size = 0;
        my $status = 0;
        my $message = "running";
        my $backupsetStatus = "unknown";
        my $startTime = $self->{dbbackupObj}->getCurrentTime();
        my $endTime = "0000-00-00 00:00:00";
        my $updateTime = $self->{dbbackupObj}->getCurrentTime();
        my $masterLogFile = "no";
        my $masterLogPos = 0;
        
        my $recordId = $self->{dbbackupObj}->insertBackupInfo($host,$port,$dbType,$bakType,$level,
            $levelValue,$startTime,$endTime,$size,$status,$message,$filerDir,$fileDir,$backupsetStatus,
            $updateTime,$isSlave,$isCompressed,$isEncrypted,$masterLogFile,$masterLogPos);
        
        $log->info("get recordId is $recordId");

        # 执行备份
        my $result = $self->runBackup($host,$port,$dbType,$bakType,$filerDir,$bakDir,$fileDir,
            $isSlave,$isEncrypted,$isCompressed,$recordId,
            $flushTableLockLimit,$detailLog,$lvmSpeed,$startTime,$dataDir);
    } else {
        $log->info("instance $host:$port backupset exists, stop backup");
    }
}

# @Description:  执行备份
# @Param:  $dbh, $sql
# @Return:  $row or undef
sub runBackup {
    my ( $self,$host,$port,$dbType,$bakType,$filerDir,$bakDir,$fileDir,
        $isSlave,$isEncrypted,$isCompressed,$recordId,
        $flushTableLockLimit,$detailLog,$lvmSpeed,$startTime,$dataDir ) = @_;
    
    my $log = Log::Log4perl->get_logger("");    

    my $isDone = 0;
    my $isSuccess = 0;
    my $isKilled = 0;
    my $isBlocked = 0;    
    
    # 变量初始化
    my $size = 0;
    my $status = 0;
    my $message = "running";
    my $backupsetStatus = "unknown";
    my $endTime = "0000-00-00 00:00:00";
    my $updateTime = $self->{dbbackupObj}->getCurrentTime();
    my $masterLogFile = "no";
    my $masterLogPos = 0;

    # 创建快照
    my $isSnapshotCreated = 0;
    my ( $lvPath,$lvName );
    my ( $fileSystem,$mountPoint );
    
    $log->info("create local lvm snapshot");
    
    # 获取实例数据对应的文件系统目录和挂载点
    ( $fileSystem,$mountPoint ) = $self->{dbbackupObj}->getLvmFsMp($host,$port,$dbType,$dataDir);
    $log->debug("get \$fileSystem: $fileSystem, \$mountPoint: $mountPoint");
    
    if ( defined($fileSystem) and defined($mountPoint) ){
        
        # 创建快照
        ( $lvPath,$lvName,$isSnapshotCreated ) = $self->{dbbackupObj}->createLvmSnapshot( $fileSystem,$mountPoint );
        $log->debug("get \$lvPath: $lvPath, \$lvName: $lvName, \$isSnapshotCreated: $isSnapshotCreated");
        
        if ( $isSnapshotCreated != 1 ){
            $log->error("create lvm snapshot failed");
        }
    }else{
        $log->error("get lvm mount point failed");
    }
    
    # 挂载快照    
    my $filesystemType = $self->{dbbackupObj}->getFilesystemType($dataDir);
    $log->debug("get \$filesystemType: $filesystemType");
    
    if ( ! defined($filesystemType) ){
        $log->warn("get dataDir: $dataDir filesystem type failed, set default filesystem type: ext4");
    }
    
    # 挂载lvm分区
    my ( $backupMountPoint,$isMounted ) = $self->{dbbackupObj}->mountSnapshot($host,$port,$dbType,$filesystemType,$lvPath,$lvName);
    if ( $isMounted == 0 ){
        $log->error("mount snapshot failed");
        
        # 挂载失败,删除lvm快照
        $log->info("remove snapshot now");
        my $isRemoved = $self->{dbbackupObj}->removeLvmSnapshot($backupMountPoint,$lvPath);
        if ( $isRemoved == 0 ){
            $log->error("remove snapshot failed, please manual remove it");
        }else{
            $log->info("remove snapshot success");
        }
    }else{
        $log->info("mount snapshot success");
    }

    # 传输数据
    $log->info("rsync data...");
    
    my ( $source,$target );
    
    $dataDir =~ s/${mountPoint}//;
    $source = "${mountPoint}/${dataDir}";
    $target = $bakDir;
    
    # rsync传输 
    my $isRsynced = $self->{dbbackupObj}->rsyncBackupset($source,$target,$lvmSpeed,$mountPoint,$lvPath,$lvName);
    if ( $isRsynced == 0){
        $log->error("rsync source: $source to target: $target failed");
        $self->{dbbackupObj}->removeDirectory($target);
    }else{
        $log->info("rsync source: $source to target: $target success");
    }

    # 更新备份资料库
    $size = $self->{dbbackupObj}->runCommand("du -s $bakDir|awk '{print \$1}'");
    $status = 1;
    $message = "rsync success";
    $backupsetStatus = "ok";
    $endTime = $self->{dbbackupObj}->getCurrentTime();
    $updateTime = $self->{dbbackupObj}->getCurrentTime();
    $masterLogFile = "no";
    $masterLogPos = 0;
           
    $self->{dbbackupObj}->updateBackupInfo($recordId,$endTime,$size,$status,$message,
        $fileDir,$backupsetStatus,$updateTime,$masterLogFile,$masterLogPos);
    
    # 删除快照
    my $isRemoved = $self->{dbbackupObj}->removeLvmSnapshot($backupMountPoint,$lvPath);
    if ( $isRemoved == 0 ){
        $log->error("remove snapshot failed, please manual remove it");
    }else{
        $log->info("remove snapshot success");
    }
    
    # 加密
    if ( lc($isEncrypted) eq "y" ){
        my $encrypt = $self->{dbbackupObj}->encryptBackup($host,$port,$dbType,$bakType,$bakDir,$startTime);
        if ( $encrypt ){
            # 加密成功
            $log->info("encrypt backupset success");
                
            # 更新备份资料库
            my $size = $self->{dbbackupObj}->runCommand("du -s $bakDir |awk '{print \$1}'");
            my $status = 1;
            my $message = "encrypt success";
            my $backupsetStatus = "ok";
            my $endTime = $self->{dbbackupObj}->getCurrentTime();
            my $updateTime = $self->{dbbackupObj}->getCurrentTime();
            my $masterLogFile = "no";
            my $masterLogPos = 0;
            
            $self->{dbbackupObj}->updateBackupInfo($recordId,$endTime,$size,$status,$message,
                $fileDir,$backupsetStatus,$updateTime,$masterLogFile,$masterLogPos);
        }else{
            # 加密失败
            $log->error("encrypt backupset failed");
            
            # 删除无效备份
            my $removeRes = $self->removeDirectory($bakDir);
            if ( $removeRes ){   
                $log->info("remove backup dir: $bakDir success");
            }else{
                $log->error("remove backup dir: $bakDir failed");
            }
                
            # 更新备份资料库
            my $size = 0;
            my $status = 2;
            my $message = "encrypt failed";
            my $backupsetStatus = "deleted";
            my $endTime = $self->{dbbackupObj}->getCurrentTime();
            my $updateTime = $self->{dbbackupObj}->getCurrentTime();
            my $masterLogFile = "no";
            my $masterLogPos = 0;
        
            $self->{dbbackupObj}->updateBackupInfo($recordId,$endTime,$size,$status,$message,
                $fileDir,$backupsetStatus,$updateTime,$masterLogFile,$masterLogPos);                
               
        }
    }
        
    # 压缩
    if ( lc($isEncrypted) eq "n" && lc($isCompressed) eq "y" ){
        my $compressFile = $self->{dbbackupObj}->compressBackup($host,$port,$dbType,$bakType,$bakDir,$startTime);
        if ( $compressFile ){
            # 压缩成功
            $log->info("compress backupset success");
              
            # 更新备份资料库
            my $size = $self->{dbbackupObj}->runCommand("du -s $bakDir|awk '{print \$1}'");
            my $status = 1;
            my $message = "compress success";
            my $backupsetStatus = "ok";
            my $endTime = $self->{dbbackupObj}->getCurrentTime();
            my $updateTime = $self->{dbbackupObj}->getCurrentTime();
            my $masterLogFile = "no";
            my $masterLogPos = 0;
 
            $self->{dbbackupObj}->updateBackupInfo($recordId,$endTime,$size,$status,$message,
                $fileDir,$backupsetStatus,$updateTime,$masterLogFile,$masterLogPos);
       
        }else{
            # 压缩失败
            $log->error("compress backupset failed");
    
            # 删除无效备份集
            $log->info("compress backupset failed, start removing backupset");
            
            my $removeRes = $self->{dbbackupObj}->removeDirectory($bakDir);
            if ( $removeRes ){   
                $log->info("remove backup dir: $bakDir success");
            }else{
                $log->error("remove backup dir $bakDir failed");
            }
            
            # 更新备份资料库
            my $size = 0;
            my $status = 2;
            my $message = "compress failed";
            my $backupsetStatus = "deleted";
            my $endTime = $self->{dbbackupObj}->getCurrentTime();
            my $updateTime = $self->{dbbackupObj}->getCurrentTime();
            my $masterLogFile = "no";
            my $masterLogPos = 0;
            
            $self->{dbbackupObj}->updateBackupInfo($recordId,$endTime,$size,$status,$message,
                $fileDir,$backupsetStatus,$updateTime,$masterLogFile,$masterLogPos);
            
            exit 1;
        }
    }
    
    return $isSuccess;
}

1;
