# Description:  mongodump backup

package Mongodump;

use strict;
use warnings;

use POSIX qw(strftime);
use POSIX qw(SIGALRM);
use File::Path qw(make_path remove_tree);
use Log::Log4perl;


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
        $isEncrypted,$isCompressed,$retention,$mongodumpExpireDays,$storageType ) = @_;

    my $log = Log::Log4perl->get_logger("");
    
    # 实例备份目录
    my $baseDir = $self->{dbbackupObj}->getStorageDir($host,$port,$dbType,$storageType);
    if ( ! $baseDir ){
        $log->error("storage dir is not mounted, stop backup");
        
        return 0;
    }
    $log->debug("get \$baseDir: $baseDir");
    
    my $fileDir = "dump/$backupDate\_$host\_$port";
    $log->debug("get \$fileDir: $fileDir");
    
    my $bakDir = "$baseDir/$fileDir";
    $log->debug("get \$bakDir: $bakDir");
    
    # 创建dump备份存储目录
    if ( ! -d "$baseDir/dump" ){
        mkdir("$baseDir/dump");
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
    if ( defined($filerDir) ){
        $log->info("get filerDir: $filerDir");
    }

    # 设置默认db
    my $defaultDb = "admin";
    
    my $forceDelete = 1;
    
    # 获取实例安装目录
    my $mongoBaseDir = $self->{dbbackupObj}->getMongoBaseDir($port);
    $log->debug("get \$mongoBaseDir: $mongoBaseDir");

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
        my $updateTime = $self->{dbbackupObj}->getCurrentTime();
        my $endTime = "0000-00-00 00:00:00";
        my $masterLogFile = "no";
        my $masterLogPos = 0;
        
        my $recordId = $self->{dbbackupObj}->insertBackupInfo($host,$port,$dbType,$bakType,$level,
            $levelValue,$startTime,$endTime,$size,$status,$message,$filerDir,$fileDir,$backupsetStatus,
            $updateTime,$isSlave,$isCompressed,$isEncrypted,$masterLogFile,$masterLogPos);

        $log->info("get recordId is $recordId");
        
        # 获取连接mongodb配置
        my $defaultConfig = $self->{dbbackupObj}->getMongoDefaultConfig();
        
        my $defaultHost = $defaultConfig->{ip};
        my $defaultPort = $port;
        my $defaultDbname = $defaultConfig->{db_name};
        my $defaultDbuser = $defaultConfig->{db_username};
        my $defaultDbpass = $defaultConfig->{db_password};
        
        # 执行备份
        my $result = $self->runBackup($host,$port,$defaultDbuser,$defaultDbpass,$bakDir,$recordId,$mongoBaseDir,$detailLog);
        
        if ( $result ){    
            # 更新备份资料库
            $size = $self->{dbbackupObj}->runCommand("du -s $bakDir|awk '{print \$1}'");
            $status = 1;
            $message = "mongodump success";
            $backupsetStatus = "ok";
            $endTime = $self->{dbbackupObj}->getCurrentTime();
            $updateTime = $self->{dbbackupObj}->getCurrentTime();
            $masterLogFile = "no";
            $masterLogPos = 0;
                   
            $self->{dbbackupObj}->updateBackupInfo($recordId,$endTime,$size,$status,$message,
                $fileDir,$backupsetStatus,$updateTime,$masterLogFile,$masterLogPos);
                
            
            if ( lc($isEncrypted) eq "y"){
                # 加密
                my $encrypt = $self->{dbbackupObj}->encryptBackup($host,$port,$dbType,$bakType,$bakDir,$startTime);
                
                if ( $encrypt ){
                    # 加密成功
                    $log->info("encrypt backupset success");
        
                    # 更新备份资料库
                    my $size = $self->{dbbackupObj}->runCommand("du -s $bakDir|awk '{print \$1}'");
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
                    # 加密备份失败
                    $log->error("encrypt backupset failed");
        
                    # 删除备份
                    $log->info("start removing backupset");
                    
                    my $removeRes = $self->{dbbackupObj}->removeDirectory($bakDir);
                    if ( $removeRes ){   
                        $log->info("remove backup dir: $bakDir success");
                    }else{
                        $log->error("remove backup dir $bakDir failed");
                    }
        
                    # 更新备份资料库备份状态 
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

            # 清理过期备份  
            if ( $retention and $mongodumpExpireDays ){
                
                # 检查目录存在
                if ( -d $bakDir ){
                    my $deleteRes = $self->{dbbackupObj}->deleteExpiredBackupSet($host,$port,$dbType,$bakType,
                        $backupDate,$bakDir,$retention,$mongodumpExpireDays,$forceDelete);
                        
                    if ( $deleteRes ){
                        $log->info("delete expired backupset success");
                    }else{
                        $log->error("delete expired backupset failed");
                    }
                }
            }
            
            $log->info("mongodump backup $host:$port success");
        }
    } else {
        $log->info("instance $host:$port backupset exists, stop backup");
    }
    
    return 1;
}

# @Description:  执行备份
# @Param:
# @Return:  1
sub runBackup {
    my ( $self,$host,$port,$dbUser,$dbPass,$bakDir,$recordId,$mongoBaseDir,$detailLog ) = @_;

    my $log = Log::Log4perl->get_logger("");

    my $mongodump = "$mongoBaseDir/bin/mongodump";
    
    my $option = "--host=$host --port=$port -u$dbUser -p$dbPass --authenticationDatabase=admin";

    my $cmd = $mongodump . " " . $option . " " . "--out=$bakDir" . " " . "--oplog";
    $log->info("$cmd");
    
    # 执行备份
    my $result = `$cmd 2>&1`;
    
    # 异常处理
    if ( $result =~ /errmsg: "auth fails"/ ){
        $log->error("errmsg: auth fails");
        
        $cmd = "$mongoBaseDir/bin/mongodump --port $port -o $bakDir --oplog";
        
        $log->info("run command: $cmd");
        
        $result = `$cmd 2>&1`;
    }
    
    if ( $result =~ /No operations in oplog/ ){
        $log->error("no operations in oplog");
        
        $cmd = "$mongoBaseDir/bin/mongodump --port $port -u $dbUser -p $dbPass -o $bakDir";

        $log->info("run command: $cmd");
        
        $result = `$cmd 2>&1`;
    }
    
    eval{
        open DETAILLOG,">>$detailLog" or $log->error("open detaillog failed");
        print DETAILLOG $result;
        close DETAILLOG;
    };
    if ($@){
        $log->error("log message to detaillog failed");
        undef $@;
    }

    return 1;
}

1;

