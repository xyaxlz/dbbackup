# Description:  mysqldump backup

package Mysqldump;

use strict;
use warnings;

use utf8;

use File::Path;
use File::Spec;
use DBI;
use IPC::Open3;
use POSIX ":sys_wait_h";
use Log::Log4perl;


# 构造函数
sub new {
    my ( $class, %args ) = @_;
    
    my $self = {}; 
    
    my $log = Log::Log4perl->get_logger(""); 

    # 接收对象
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

# @Description:  主程序
# @Param:
# @Return:
sub main {
    my ( $self,$host,$port,$dbType,$isSlave,$bakType,$level,$levelValue,$backupDate,
        $isEncrypted,$isCompressed,$retention,$mysqldumpExpireDays,$storageType ) = @_;
        
    my $log = Log::Log4perl->get_logger("");
    
    # 获取mysqldump flush table lock参数值
    my $flushTableLockLimit = $self->{dbconfigObj}->get('dumpFlushTableLockLimit');

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
    
    my $statusFile = "/tmp/$backupDate\_$host\_$port\_$bakType\_$level.done";
    $log->debug("get \$statusFile: $statusFile");
    
    my $mysqldumpScript = "/tmp/$backupDate\_$host\_$port\_$bakType\_$level.sh";
    $log->debug("get \$mysqldumpScript: $mysqldumpScript");
    
    my $forceDelete = 1;

    # 创建mysqldump备份存储目录
    if ( ! -d "$baseDir/dump" ){
        mkdir("$baseDir/dump");
        $log->info("create dir: $baseDir/dump success");
    }
    
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

    # 获取连接mysql配置
    my $defaultConfig = $self->{dbbackupObj}->getMysqlDefaultConfig();
    
    my $defaultHost = $defaultConfig->{ip};
    my $defaultPort = $port;
    my $defaultDbname = $defaultConfig->{db_name};
    my $defaultDbuser = $defaultConfig->{db_username};
    my $defaultDbpass = $defaultConfig->{db_password};

    # 连接mysql
    my $dbh = $self->{dbbackupObj}->mysqlConnect($defaultHost,$defaultPort,
        $defaultDbname,$defaultDbuser,$defaultDbpass);

    # 获取mysql安装目录
    my $mysqlBasedir = $self->{dbbackupObj}->getMysqlVariableValue($dbh,"select \@\@basedir");

    # 获取mysql bin-log开启情况
    my $isLogBin = $self->{dbbackupObj}->getMysqlVariableValue($dbh,"select \@\@log_bin");

    # 获取mysql版本
    my $version = $self->{dbbackupObj}->getMysqlVariableValue($dbh,"select \@\@version");

    # 版本前缀
    my $versionPrefix = (split(/-/,$version))[0];
    $versionPrefix =~ s/\.//g;

    # 获取mysqldump split脚本
    my $splitScript = $self->{dbconfigObj}->get('mysqldumpSplitScript');
    if ( ! -e $splitScript ){
        $log->error("mysqldump split script: $splitScript not exist");
    }
    $log->info("get mysqldump split script: $splitScript success");

    # 获取mytab文件
    my $mytab = $self->{dbconfigObj}->get('mytab');
    $log->debug("get \$mytab: $mytab");
    
    # 获取my.cnf配置文件
    my $myCnf = $self->{dbbackupObj}->getMycnf($port,$mytab);
    $log->debug("get \$myCnf: $myCnf");
    
    # 根据备份级别创建备份脚本
    my @itemList = $levelValue;

    my ( $db,$table );
    
    # mysqldump命令
    my $mysqldump = $self->{dbbackupObj}->runCommand("which mysqldump");
    chomp($mysqldump);
    if ( ! $mysqldump ){
        $mysqldump = "/usr/bin/mysqldump";
    }
    $log->debug("get \$mysqldump: $mysqldump");    
    $mysqldump = $mysqldump . " -f ";
    
    # 备份级别值
    if ( !$levelValue ){
        $levelValue = "null";
    }
    
    # 备份级别选项
    my $optionLevel;
    
    # 数据库级备份
    if ( lc($level) eq "db" ){
        $optionLevel = "-R --databases $levelValue";
    
    # 表级备份
    } elsif ( lc($level) eq "table" ){
        ($db, $table) = split(/./, $levelValue);
        $optionLevel = "--databases $db --tables $table";
    } else {
        $levelValue = 'noset';
        $optionLevel = "--all-databases";
    }
    
    # 状态文件
    if ( -f $statusFile ){
        unlink $statusFile;
    }

    open STATUSFILE, '>', $statusFile;
    print STATUSFILE 1;
    close(STATUSFILE);
    
    $log->info("backup status file:  $statusFile");
    
    my $option;  # 备份选项
    my $optionLog = "";  # 备份日志选项
    
    # mysql版本<= 4020
    if ( $versionPrefix <= 4020 ){
        $option = "--opt --skip-add-drop-table --single-transaction --default-character-set=latin1";
    } else {
        $option = "--opt --skip-add-drop-table --single-transaction --hex-blob --default-character-set=binary";
    }

    # mysql版本大于5188
    if ( $versionPrefix >= 5118 ){
        $optionLog = " -v --log-error=$detailLog";
    }
    
    $option = "$option $optionLevel $optionLog -h$defaultHost -P$defaultPort -u$defaultDbuser -p\'$defaultDbpass\'";
   
    if ( $isLogBin ){
        $option = "$option --master-data=2";
    }
    
    my $optionDdl = "--opt --single-transaction --default-character-set=binary --all-databases --no-data -h$defaultHost -P$defaultPort -u$defaultDbuser -p\'$defaultDbpass\'";
    my $optionTableDdl = '--no-data';
    my $optionTableData = '--skip-add-drop-table --no-create-info';

    if ( -f $mysqldumpScript ){
        unlink $mysqldumpScript;
    }
    
    open ( MYSQLDUMPSCRIPT,">>",$mysqldumpScript ) or $log->error("create mysqldump script: $mysqldumpScript failed");
    
    if ( lc($level) eq "table" ){
        print MYSQLDUMPSCRIPT "$mysqldump $option $optionTableDdl > $bakDir/table_define.$db.$table.sql\n";
    } else {
        print MYSQLDUMPSCRIPT "cp $myCnf ${bakDir}/my_${port}.cnf\n";
        print MYSQLDUMPSCRIPT "$mysqldump $optionDdl >$bakDir/full_no_data.dmp\n";
        print MYSQLDUMPSCRIPT "$mysqldump $option|OUTDIR=$bakDir $splitScript\n";   
    }
    
    print MYSQLDUMPSCRIPT "echo \$? > $statusFile\n";
    close(MYSQLDUMPSCRIPT);
    
    $log->info("create mysqldump script :$mysqldumpScript success");

    # 检查备份
    my $isExistBackupset = $self->{dbbackupObj}->isExistBackupset($host,$port,$dbType,$bakType,$bakDir,$fileDir);
    chomp($isExistBackupset);
    $log->debug("get \$isExistBackupset: $isExistBackupset");

    if ( $isExistBackupset == 0 ){
    
        # 更新备份资料库
        my $size = 0;
        my $status = 0;
        my $message = "running";
        my $backupsetStatus = "";
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
        my $result = $self->runBackup($defaultHost,$defaultPort,$defaultDbname,$defaultDbuser,
            $defaultDbpass,$bakType,$filerDir,$bakDir,$fileDir,$isSlave,$isCompressed,$recordId,
            $flushTableLockLimit,$detailLog,$statusFile,$mysqldumpScript,$versionPrefix,$dbh );
        if ( $result ){
            if ( lc($isEncrypted) eq "y" ){
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
                         
                } else {
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
                $log->info("gzip backup files: $bakDir");
                system("find $bakDir -name \"*\" -type f -exec gzip {} \\;");
                
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
            }
    
            # 清理过期备份  
            if ( $retention and $mysqldumpExpireDays ){
                
                # 检查目录存在
                if ( -d $bakDir ){
                    my $deleteRes = $self->{dbbackupObj}->deleteExpiredBackupSet($host,$port,$dbType,$bakType,
                        $backupDate,$bakDir,$retention,$mysqldumpExpireDays,$forceDelete);
                        
                    if ( $deleteRes ){
                        $log->info("delete expired backupset success");
                    }else{
                        $log->error("delete expired backupset failed");
                    }
                }
            }
            
            $log->info("mysqldump success");
        }
    } else {
        $log->info("instance $host:$port backupset exists, stop backup");
    }
}

# @Description:  执行备份
# @Param:  $dbh, $sql
# @Return:  $isSuccess
sub runBackup {
    my ( $self,$defaultHost,$defaultPort,$defaultDbname,$defaultDbuser,$defaultDbpass,
        $bakType,$filerDir,$bakDir,$fileDir,$isSlave,$isCompressed,$recordId,
        $flushTableLockLimit,$detailLog,$statusFile,$mysqldumpScript,$versionCode,$dbh ) = @_;
    
    my $log = Log::Log4perl->get_logger("");    

    my $isDone = 0;
    my $isSuccess = 0;
    my $isKilled = 0;
    my $isBlocked = 0;    

    # 检查mysql客户端命令
    my $mysql = $self->{dbbackupObj}->runCommand("which mysql");
    chomp($mysql);
    if ( ! $mysql ){
        $mysql = "/usr/bin/mysql";
    }
    $log->debug("get \$mysql: $mysql");

    my $mysqlDefaultConn = "$mysql -h$defaultHost -P$defaultPort -u$defaultDbuser -p\'$defaultDbpass\'";
    
    # 更新备份资料库
    my $size = 0;
    my $status = 0;
    my $message = "running";
    my $backupsetStatus = "unknown";
    my $endTime = $self->{dbbackupObj}->getCurrentTime();
    my $updateTime = $self->{dbbackupObj}->getCurrentTime();
    my $masterLogFile = "no";
    my $masterLogPos = 0;
        
    $self->{dbbackupObj}->updateBackupInfo($recordId,$endTime,$size,$status,$message,
        $fileDir,$backupsetStatus,$updateTime,$masterLogFile,$masterLogPos); 

    my $count = 0;
    for ( $count=1;$count<=3;$count++ ){   
        # 创建备份进程
        local ( *CHLD_IN,*CHLD_OUT,*CHLD_ERR );
        my $backupPid = open3( *CHLD_IN,*CHLD_OUT,*CHLD_ERR,"/bin/bash","$mysqldumpScript" );
    
        close CHLD_IN;
        close CHLD_OUT;
        close CHLD_ERR;
        
        $log->debug("backup process is: $backupPid");
        
        # 监控备份执行过程
        while (1){
            my $isBlocked = 0;
            my $getProcesslist = $self->{dbbackupObj}->getMysqlVariableValue($dbh,"show processlist");
            if ( !$getProcesslist ){
                # 检查mysql异常, 杀掉备份进程
                $log->error("check mysql load failed, kill backup process");
                $self->{dbbackupObj}->runCommand("pkill -TERM -P $backupPid");
                sync() or $log->warn("call sync() failed");
                
                # 清理备份目录
                if ( -d $bakDir ){
                    rmtree($bakDir);
                }
                
                last;
            }else{
                sleep 15;
                $log->info("waiting for mysqldump ending");
            }
            
            # 获取脚本执行退出状态码
            open BAKSTAT,$statusFile;
            my $mysqldumpExitCode = <BAKSTAT>;
            close BAKSTAT;

            $log->info("mysqldump script exit code: $mysqldumpExitCode");
            
            chomp($mysqldumpExitCode);
            if ( $mysqldumpExitCode == 0 ){
                $isDone = 1;
                last;
            }
            
            my $kid = 0;
            $kid = waitpid($backupPid, WNOHANG);
            $log->debug("kid is $kid ; backupPid is $backupPid");
                        
            if ( $kid == $backupPid or $kid == -1 ){
                $log->info("backup child process $kid end");
                if ( $isDone == 0 ){
                    $log->warn("child process is killed");
                    $isKilled = 1;
                }
                
                last;
            }
    
            # 检查命令
            my $checkCmd = "$mysqlDefaultConn -e 'show processlist' |egrep -iE 'Flush tables' |awk '{print \$1,\$6}'|sort -r |head -1";
            my $procStat = system("$checkCmd");
            $log->debug("get \$procStat: $procStat");
            
            my ($sessionNo,$tableLock) = split(/\s+/, $procStat);
            if ( !defined($tableLock) ){
                $tableLock = 0;
            }
            if ( !defined($sessionNo) ){
                $sessionNo = 0;
            }
            
            $log->info("processlist status:$procStat,sessionNo:$sessionNo,table lock time: $tableLock");
            
            if ( ($tableLock+60) > $flushTableLockLimit ){
                $log->error("flush tables longer than $flushTableLockLimit");
                $log->warn("kill child process and mysql session");
                
                $self->{dbbackupObj}->runCommand("pkill -TERM -P $backupPid");
                $self->{dbbackupObj}->runCommand("$mysqlDefaultConn -e 'kill $sessionNo'");
                
                $isBlocked = 1;
                
                last;
            }
        }

        if ( $isDone ){
            last;
        }
    }
    
    # 检查备份日志
    if ( $versionCode > 5118 ){
    # mysql版本大于5118
        my $errs;
        if ( -f $detailLog ){
            open DETAIL,'<',$detailLog;
            my @errs = <DETAIL>;
            close DETAIL;
            
            my $errs = qq(@errs);
            if ( $errs =~ m/error/ ){
                if ( $errs =~ m/^mysqldump/ ){
                    $isSuccess = 0;
                    $log->error("mysql error found in $detailLog");
                }else{
                    $isSuccess = 1;
                }
            } else {
                $isSuccess = 1;
            }
        } else {
            $log->error("no mysqldump logfile");
            $isSuccess = 0;
        }
    } else {
        if ( $isDone ){
            $log->info("backup success");
            $isSuccess = 1;
        } else {
            $log->error("backup failed");
            $isSuccess = 0;
        }
    }
    
    # 被blocked
    if ( $isBlocked ){
        $isSuccess = 0;

        $size = 0;
        $status = 2;
        $message = "flush_tables_timeout";   
        $backupsetStatus = "deleted";
    }
    
    # 被kill掉
    if ( $isKilled ){
        $isSuccess = 0;

        $size = 0;
        $status = 2;
        $message = "mysqldump_been_killed";   
        $backupsetStatus = "deleted";
    }
    
    # 备份成功
    if ( ($isDone && $isSuccess) ){
        $size = $self->{dbbackupObj}->runCommand("du -s $bakDir|awk '{print \$1}'");
        $status = 1;
        $message = "success";    
        $backupsetStatus = "ok";
    } else {
        # 删除备份
        $log->info("start removing backupset");
        
        my $removeRes = $self->{dbbackupObj}->removeDirectory($bakDir);
        if ( $removeRes ){   
            $log->info("remove backup dir: $bakDir success");
        }else{
            $log->error("remove backup dir $bakDir failed");
        }
    }

    # 更新备份资料库
    $endTime = $self->{dbbackupObj}->getCurrentTime();
    $updateTime = $self->{dbbackupObj}->getCurrentTime();  
    $masterLogFile = "no";
    $masterLogPos = 0;
    
    $self->{dbbackupObj}->updateBackupInfo($recordId,$endTime,$size,$status,$message,
        $fileDir,$backupsetStatus,$updateTime,$masterLogFile,$masterLogPos);
            
    # 删除临时文件
    unlink $mysqldumpScript or $log->warn("remove $mysqldumpScript failed"); 
    unlink $statusFile or $log->warn("remove $statusFile failed");
    
    return $isSuccess;
}

1;
