# Description:  ftp backup

package Ftp;

use strict;
use warnings;

use DBI;
use File::Spec;
use File::Copy;
use File::Find;
use Digest::MD5 qw(md5 md5_hex md5_base64);
use Fcntl qw(:flock);
use POSIX qw(strftime);
use POSIX qw(:signal_h);
use IPC::Open3;
use Symbol 'gensym';
use Net::FTP;
use Log::Log4perl;


# 构造函数
sub new {
    my ( $class, %args ) = @_;
    
    my $self = {};
    
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


# 主程序
sub main {
    my ( $self,$host,$port,$dbType,$isSlave,$bakType,$level,$levelValue,$backupDate,
        $isEncrypted,$isCompressed,$retention,$storageType) = @_;
                
    my $log = Log::Log4perl->get_logger("");

    $log->info("start ftp backup");

    # 获取ftp用户名和密码
    my $ftpUser = $self->{dbconfigObj}->get("ftpuser");
    my $ftpPass = $self->{dbconfigObj}->get("ftppass");
    
    # 获取ftp服务器ip
    my $ftpServer = $self->{dbbackupObj}->getFtpServer($host,$port);
    $log->debug("get \$ftpServer: $ftpServer");
    
    # 实例备份目录
    my $baseDir = $self->{dbbackupObj}->getStorageDir($host,$port,$dbType,$storageType);
    if ( ! $baseDir ){
        $log->error("storage dir is not mounted, stop backup");
        return 0;
    }
    $log->debug("get \$baseDir: $baseDir");

    my $fileDir = "dump/$backupDate\_$host\_$port";
    my $bakDir = "$baseDir/$fileDir";

    # 日志目录
    if ( ! -d "/home/mysql/dbadmin/logs/$backupDate" ){
        mkdir("/home/mysql/dbadmin/logs/$backupDate");
    }
    
    # 日志文件
    my $logPrefix = "/home/mysql/dbadmin/logs/$backupDate/$backupDate\_$host\_$port\_$bakType";
    my $mainLog = "$logPrefix.log";
    my $detailLog = "$logPrefix.detail";
    my $traceLog = "$logPrefix.trace";

    # 获取filer存储目录
    my $filerDir = $self->{dbbackupObj}->getFilerDir($host,$port,$dbType,$storageType);
    $log->info("get filerDir: $filerDir");

    # 获取当前备份中最新的备份集
    my $latestBackupDir = $self->{dbbackupObj}->getLatestBackupSet($host,$port,$dbType,$storageType);
    if ( !defined($latestBackupDir) ){
        $log->error("get latest backupset failed");

        return 0;
    }
    $log->debug("get \$latestBackupDir: $latestBackupDir");

    # 获取上次ftp备份时间
    my $lastFtpBackupTime = $self->{dbbackupObj}->getLastFtpBackupTime($host,$port,$ftpServer);
    $log->debug("get \$lastFtpBackupTime: $lastFtpBackupTime");
    
    # 根据ftp上最新备份和当前日期计算是否需要上传新备份来满足保留策略
    my $isNeedUpload = $self->{dbbackupObj}->isNeedUploadBackupSet($lastFtpBackupTime,$retention);
    if ( defined($isNeedUpload) ){
        # 需要ftp备份
        $log->info("we need upload new backupSet to ftp server");
    }else{
        # 不需要ftp备份
        $log->info("there is enough backupSet on ftp server, no need upload");
        
        return 0;
    }

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

    # 上传备份数据到ftp服务器
    my $result = $self->runFtpBackup($host,$port,$latestBackupDir,$ftpServer,$ftpUser,$ftpPass,$detailLog);
    if ( defined($result) ){
        $log->info("upload new backupSet to ftp server success");

        # 记录ftp备份日志到ftp_backup_info表
        my $filerIp = $self->{dbbackupObj}->getMfsServerIp($host,$port,$dbType);
        if ( !defined($filerIp) ){
            $log->warn("get mfs server ip failed, but upload backupSet to ftp server success");
        }

        # 更新备份资料库
        $size = $self->{dbbackupObj}->runCommand("du -s $latestBackupDir |awk '{print \$1}'");
        $status = 1;
        $message = "ftp backup success";
        $backupsetStatus = "ok";
        $endTime = $self->{dbbackupObj}->getCurrentTime();
        $updateTime = $self->{dbbackupObj}->getCurrentTime();
        $masterLogFile = "no";
        $masterLogPos = 0;
        
        $self->{dbbackupObj}->updateBackupInfo($recordId,$endTime,$size,$status,$message,
            $fileDir,$backupsetStatus,$updateTime,$masterLogFile,$masterLogPos);
        
        # 更新ftp_backup_info表
        $self->{dbbackupObj}->updateFtpBackupInfo($host,$port,$filerIp,$latestBackupDir,$ftpServer);
        
    }else{
        $log->error("upload backupset to ftp server failed");
    	        
        # 更新备份资料库
        $size = 0;
        $status = 2;
        $message = "ftp backup failed";
        $backupsetStatus = "unknown";
        $endTime = $self->{dbbackupObj}->getCurrentTime();
        $updateTime = $self->{dbbackupObj}->getCurrentTime();
        $masterLogFile = "no";
        $masterLogPos = 0;
        
        $self->{dbbackupObj}->updateBackupInfo($recordId,$endTime,$size,$status,$message,
            $fileDir,$backupsetStatus,$updateTime,$masterLogFile,$masterLogPos);
    }
    
    # 删除ftp server过期备份集
    my $deleteExpireBackupSet = $self->{dbbackupObj}->deleteFtpExpireBackupSet($host,$port,$retention,$ftpServer,$ftpUser,$ftpPass);
    if ( $deleteExpireBackupSet ){
        $log->info("delete expire backupSet success");
    }else{
        $log->warn("delete expire backupSet failed");
    }
     
    $log->info("ftp backup success"); 
}

# @Description:  执行ftp备份
# @Param:
# @Return:  1:success  0:failed
sub runFtpBackup {
    my ( $self,$host,$port,$backupSetDir,$ftpServer,$ftpUser,$ftpPass,$detailLog ) = @_;

    my $log = Log::Log4perl->get_logger("");
    
    $log->debug("funFtpBackup get \$backupSetDir: $backupSetDir");
    
    # backupSet路径范例: /home/mysql/backup_stage/10.100.20.37_3306/dump/20150801_10.100.20.37_3306 
    my @pathArr = split(/\//, $backupSetDir);
    my $newestDir;
    foreach my $line ( @pathArr ){
        if ( $line =~ /\d+\_\d+\.\d+\.\d+\.\d+\_\d+/ ){
            $newestDir = $line;
            chomp($newestDir);
            $log->info("newest backupSetDir: $newestDir");
        }
    }

    # 连接ftp server
    my $errMsg;
    my $ftp = Net::FTP->new($ftpServer, Timeout=>3600);
    if ( $ftp ){
        $ftp->login($ftpUser, $ftpPass) or do{
            $errMsg = $ftp->message;
            $log->error("connect to $ftpServer failed, error msg:$errMsg");
            
            return 0;
        };
        
        $log->info("connect to $ftpServer success");
    }else{
        $errMsg = $ftp->message;
        $log->error("connect to $ftpServer failed, error msg:$errMsg");
        
        return 0;
    }
	
    # 检查ftp目录
    my $ftpDir = $host."_".$port;
    if ( $ftp->cwd("/$ftpDir/$newestDir") ){
        # ftp目录存在
        $log->warn("/$ftpDir/$newestDir is already exist on ftp server");
        $ftp->cdup();
        
        # 重新创建目录
        if ( $ftp->rmdir("/$ftpDir/$newestDir","RECURSE") ){
            # 删除目录成功
            $log->info("remove directory $ftpDir/$newestDir success");
            
            # 重新创建目录
            if ( $ftp->mkdir("/$ftpDir/$newestDir","RECURSE") ){
                # 创建目录成功
                $log->info("create directory: $ftpDir/$newestDir success");	
                	
            }else{
                $log->error("create directory: $ftpDir/$newestDir failed");
                
                return 0;	    
			}
        }else{
            $errMsg = $ftp->message;
            $log->error("remove directory: $ftpDir/$newestDir failed, error msg: $errMsg");
            
            return 0;
        }
    }else{
        # ftp目录不存在
        if ( $ftp->mkdir("/$ftpDir/$newestDir","RECURSE") ){
            # 创建目录成功
            $log->info("create directory: $ftpDir/$newestDir on ftp server success");
            
        }else{
            $errMsg = $ftp->message;
            $log->error("create dir: $ftpDir/$newestDir failed on ftp server，error msg: $errMsg");
            
            return 0;
        }
    }
    
    # 进入ftp目录
    if ( $ftp->cwd("/$ftpDir/$newestDir") ){
        $log->info("enter directory: /$ftpDir/$newestDir");
    }else{
        $errMsg = $ftp->message;
        $log->error("enter directory: /$ftpDir/$newestDir failed, error msg: $errMsg");
    }

    open( LOGDETAIL,">$detailLog" ) or do {
        # 打开detailLog文件失败, 结束ftp备份
        $log->error("open ftp log failed: $detailLog");
        
        return 0;
    };

    # 开始上传备份文件
    print LOGDETAIL "uploading files...\n";

    $ftp->binary();
    eval{
        my ( @files, @folders, @failFiles );
        
        find( { wanted => sub{
            if ( -d $File::Find::name ){
                if ( $File::Find::name ne $backupSetDir ){
                    push (@folders,substr($File::Find::name,index($File::Find::name,$backupSetDir) + length($backupSetDir)+1));
                }
            }else{
                push(@files,substr($File::Find::name,index($File::Find::name,$backupSetDir) + length($backupSetDir)+1));
            }
        }},$backupSetDir);
		
        print LOGDETAIL "backupSet dir count:", scalar @folders, "\n";
        print LOGDETAIL "backupSet file count:", scalar @files, "\n";
		
        # 创建目录
        foreach my $folder (@folders){
            print LOGDETAIL "make directory: $folder\n";
            my $mkResult = $ftp->mkdir($folder,1);
        }
        
        # 逐个上传备份文件
        foreach my $file (@files){
            print LOGDETAIL "upload $file\n";
            
            # 备份文件绝对路径
            my $bakFile = "$backupSetDir/$file";
            
            # 上传文件
            my $uploadResult = $ftp->put($bakFile,$file);
            
            my $bakFileSize = -s $bakFile;			
            
            if ( $uploadResult eq $file and $ftp->size($uploadResult) == $bakFileSize ){         
                print LOGDETAIL "upload backup file: $file success, file size: $bakFileSize\n";  
            }else{
                $errMsg = $ftp->message;
                print LOGDETAIL "upload backup file: $file failed, error msg: $errMsg\n";
                
                # 上传失败文件, 加入失败列表, 以便失败重传
                push @failFiles, $file;
            }
        }
        
        # 失败重新上传 
        my @errRetry;
        my $length = scalar(@failFiles);
        if ( $length>0 ){
            foreach my $failFile ( @failFiles ){
                my $retryFile = "$backupSetDir/$failFile";
                if ( $ftp->put($retryFile,$failFile)){
                    # 重传成功
                    print LOGDETAIL "upload backup file: $failFile success\n";
                    
                }else{
                    # 重传失败
                    $errMsg = $ftp->message;
                    print LOGDETAIL "upload backup file: $failFile failed, error msg: $errMsg\n";
                    
                    # 加入重传失败列表
                    push @errRetry,$failFile;
                }
            }
            
            # 检查重传结果
            if ( scalar(@errRetry) > 0 ){
                # 存在重传失败
                print LOGDETAIL "upload backup file failed\n";
                print LOGDETAIL "remove backupSet dir on ftp server\n";
                
                # 删除ftp目录
                $ftp->rmdir("/$ftpDir/$newestDir","RECURSE");

                return 0;
            }else{
                print LOGDETAIL "upload all backupSet files success\n";
                
                return 1;
            }
        }else{
            print LOGDETAIL "upload all backupSet files success\n";
            
            return 1;
        }
    };
    if ($@){
        print LOGDETAIL "upload backupSet files to ftp server error, error msg: $@\n";
        if ( $ftp ){
            $ftp->close; 
        }
        
        return 0;
    }
    $ftp->close; 

    return 1;
}

1;
