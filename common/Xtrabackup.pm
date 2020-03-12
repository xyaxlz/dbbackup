# Description:  mysql xtrabackup 

package Xtrabackup;

use strict;
use warnings;

use utf8;
use POSIX ":sys_wait_h";

use File::Path qw(make_path remove_tree);
use DBI;
use IPC::Open3;
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

# @Description:  xtrabackup主程序
# @Param: 
# @Return:  $isSuccess
sub main {
    my ( $self,$host,$port,$dbType,$isSlave,$bakType,$level,$levelValue,$backupDate,
        $isEncrypted,$isCompressed,$retention,$mysqlHotbakExpireDays,
        $mysqlHotbakThrottle,$storageType ) = @_;
        
    my $log = Log::Log4perl->get_logger(""); 
         
    # 实例备份目录
    my $baseDir = $self->{dbbackupObj}->getStorageDir($host,$port,$dbType,$storageType);
    if ( ! $baseDir ){
        $log->error("storage dir is not mounted, stop backup");
        return 0;
    }
    $log->debug("get \$baseDir: $baseDir");

    # 备份文件目录
    my $fileDir = "hotbak/$backupDate\_$host\_$port";
    
    # 备份目录绝对路径
    my $bakDir = "$baseDir/$fileDir";
    
    # 备份脚本 
    my $tmpBackupScript = "/tmp/$backupDate\_$host\_$port\_$bakType\_$level.sh";
    
    # 备份状态文件
    my $statusFile = "/tmp/$backupDate\_$host\_$port\_$bakType\_$level.done";

    my $forceDelete = 1;
            
    # 检查创建目录
    if ( ! -d "$baseDir/hotbak" ){
        $log->info("create dir ${baseDir}/hotbak");
        mkdir("$baseDir/hotbak");
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

    # 获取配置文件
    my $myCnf;
    
    my $myTab = $self->{dbconfigObj}->get('mytab');
    $log->info("get mytab: $myTab");
    
    open MYTAB,"<",$myTab or $log->warn("open file $myTab failed");
    while ( my $line=<MYTAB> ){
        if ( $line =~ m/$port/ ){
            $myCnf = (split(/\s+/, $line))[1];
        }
    }
    close(MYTAB);
    $log->info("get mycnf file: $myCnf");
    
    # 检查备份目录
    if ( -d $bakDir ){
        if ( $self->{dbbackupObj}->isDirEmpty($bakDir) ){
            $log->info("backup dir: $bakDir exists and is empty");
            rmtree($bakDir);
        }else{
            $log->error("backup dir already exists");
        }
    }
    
    if ( lc($isSlave) eq "n" ){
        $log->error("backup is not allowed on master node");
    }
    
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

    # 获取服务器timeout时间
    my $waitTimeout = $self->{dbbackupObj}->getMysqlVariableValue($dbh,"select \@\@wait_timeout");
    my $interactiveTimeout = $self->{dbbackupObj}->getMysqlVariableValue($dbh,"select \@\@interactive_timeout");
    
    # 获取filer存储目录
    my $filerDir = $self->{dbbackupObj}->getFilerDir($host,$port,$dbType,$storageType);
    $log->info("get filerDir is: $filerDir");

    # 检查innodb存储引擎
    my $hasInnodbSql = "SELECT SUPPORT FROM INFORMATION_SCHEMA.ENGINES WHERE ENGINE='InnoDB'";
    my $hasInnodb = $self->{dbbackupObj}->getMysqlVariableValue($dbh,$hasInnodbSql);
    if ( lc($hasInnodb) eq "no" ){
        $log->error("does not support innodb engine");
    }
    $log->debug("\$hasInnodbSql value: $hasInnodbSql");
    $log->debug("\$hasInnodb value: $hasInnodb");
    
    # 获取xtrabackup flush table lock最大时间
    my $flushTableLockLimit = $self->{dbconfigObj}->get('hotbakFlushTableLockLimit');
     
    # 检查xtrabackup安装
    if ( ! -e "/usr/bin/xtrabackup" ){
        $log->error("xtrabackup is not installed");
    }
    $log->info("xtrabackup is already installed");
    
    # 清理临时文件
    if ( -f "/tmp/$port.cnf" ){
        unlink "/tmp/$port.cnf";
    }

    open(my $bakCnf,">>","/tmp/$port.cnf") or $log->error("create config file for xtrabackup failed");
    print $bakCnf "[mysqld] \n";
    
    my @parameterList = ("basedir","datadir","innodb_log_files_in_group","innodb_log_file_size",
        "innodb_data_home_dir","innodb_data_file_path","innodb_log_group_home_dir");
    for my $var (@parameterList){
        my $varValue = $self->{dbbackupObj}->getMysqlVariableValue($dbh,"select \@\@$var");
        if ( $varValue ){
            print $bakCnf "$var=$varValue\n";
        }else{
            next;
        }
    }
    close($bakCnf);
    
    # 清理状态文件
    if ( -f $statusFile ){
        unlink $statusFile;
    }
    
    # 设置备份状态文件
    open STATUSFILE,'>',$statusFile;
    print STATUSFILE 1;
    close STATUSFILE;
    $log->info("backup status file: $statusFile");
    
    # 创建备份选项
    my $option = " --host=127.0.0.1 --throttle=$mysqlHotbakThrottle --user=$defaultDbuser"  . " ";
        $option .= " --port=$port --defaults-file=/tmp/$port.cnf --slave-info --no-timestamp"  . " ";
    
    if ( $defaultDbpass ){
        $option = "$option --password=\'$defaultDbpass\'"  . " ";
    }

    my $noLock = 0;
    if ( $noLock ){
        $option = "$option --no-lock"  . " ";
    }
    
    # 数据库级备份
    if ( lc($level) eq "db" ){
        $option = "$option --databases=$levelValue --include=$levelValue"  . " ";
    }
    
    # 表级备份
    if ( lc($level) eq "table"){
        $option = "$option --include=$levelValue" . " ";
    }

    # 创建备份脚本
    if ( -f $tmpBackupScript ){
        unlink $tmpBackupScript;
    }
    
    open (my $tmpScript,">>",$tmpBackupScript) or $log->error("create temp script file: $tmpBackupScript failed");
    print $tmpScript "innobackupex $option $bakDir 1>>$detailLog 2>&1 \n";
    print $tmpScript "echo \$? > $statusFile \n";
    print $tmpScript "cp $myCnf ${bakDir}/my_${port}.cnf\n";
    close($tmpScript);
    
    $log->info("create temp script: $tmpBackupScript");

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
        my $endTime = $self->{dbbackupObj}->getCurrentTime();
        my $updateTime = $self->{dbbackupObj}->getCurrentTime();
        my $masterLogFile = "no";
        my $masterLogPos = 0;
    
        my $recordId = $self->{dbbackupObj}->insertBackupInfo($host,$port,$dbType,$bakType,$level,
            $levelValue,$startTime,$endTime,$size,$status,$message,$filerDir,$fileDir,$backupsetStatus,
            $updateTime,$isSlave,$isCompressed,$isEncrypted,$masterLogFile,$masterLogPos);
    
        $log->info("get recordId is $recordId");
        
        # 执行备份
        my $result = $self->runBackup($defaultHost,$defaultPort,$defaultDbname,$defaultDbuser,
            $defaultDbpass,$bakType,$filerDir,$bakDir,$fileDir,$isSlave,$recordId,
            $flushTableLockLimit,$detailLog,$statusFile,$tmpBackupScript,$dbh);         
        if ( $result ){
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
            
            # 清理过期备份  
            if ( $retention and $mysqlHotbakExpireDays ){
                
                my $deleteRes = $self->{dbbackupObj}->deleteExpiredBackupSet($host,$port,$dbType,$bakType,
                    $backupDate,$bakDir,$retention,$mysqlHotbakExpireDays,$forceDelete);
                    
                if ( $deleteRes ){
                    $log->info("delete expired backupset success");
                }else{
                    $log->error("delete expired backupset failed");
                }
            }
        }
    
        $log->info("xtrabackup success");
        
    } else {
        $log->info("instance $host:$port backupset exists, stop backup");
    }
}


# @Description:  执行备份
# @Param:
# @Return:  $row or undef
sub runBackup {
    my ( $self,$defaultHost,$defaultPort,$defaultDbname,$defaultDbuser,$defaultDbpass,
        $bakType,$filerDir,$bakDir,$fileDir,$isSlave,$recordId,
        $flushTableLockLimit,$detailLog,$statusFile,$tmpBackupScript,$dbh ) = @_;

    my $log = Log::Log4perl->get_logger("");
    
    my $isDone = 0;
    my $isSuccess = 0;
    my $isKilled = 0;
    my $count = 0;
    my $isBlocked = 0;
    my $mysql = $self->{dbbackupObj}->runCommand("which mysql");
    chomp($mysql);
    if ( ! $mysql ){
        $mysql = "/usr/bin/mysql";
    }
    $log->debug("get \$mysql: $mysql");
    
    my $mysqlDefaultConn = "$mysql -h$defaultHost -P$defaultPort -u$defaultDbuser -p\'$defaultDbpass\'";
    
    my $waitTimeout = $self->{dbbackupObj}->getMysqlVariableValue($dbh,"select \@\@wait_timeout");
    my $interactiveTimeout = $self->{dbbackupObj}->getMysqlVariableValue($dbh,"select \@\@interactive_timeout");

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
    
    # 执行备份
    for ( $count=1;$count<=3;$count++ ){
        $log->info("$count/3 time");
        if ( -d $bakDir ){
            $log->info("backupset $bakDir exist, delete it");
            rmtree($bakDir);
        }
        
        # 设置连接超时时间
        $self->{dbbackupObj}->runCommand("$mysqlDefaultConn -e 'set global interactive_timeout=36000;set global wait_timeout=36000'");
        
        # 创建备份进程
        local ( *CHLD_IN,*CHLD_OUT,*CHLD_ERR );
        my $backupPid = open3( *CHLD_IN,*CHLD_OUT,*CHLD_ERR,"/bin/bash","$tmpBackupScript" );
        close CHLD_IN;
        close CHLD_OUT;
        close CHLD_ERR;
        
        $log->info("backup process id: $backupPid");
        
        # 监控备份执行过程
        while (1){
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
                $log->info("waiting for xtrabackup ending");
            }
            
            # 获取备份结果
            open BACKUPRESULT,"<",$statusFile or $log->warn("open file $statusFile failed");
            foreach my $statusFileLine (<BACKUPRESULT>){
                chomp($statusFileLine);
                $statusFileLine =~ s/^\s+//g;
                $isDone = $statusFileLine;
                last;
            }
            close BACKUPRESULT;
            
            $log->info("backup result: $isDone");
            if ( $isDone == 0 ){
                $isDone = 1;
                last;
            }
            
            my $kid = 0;
            $kid = waitpid($backupPid, WNOHANG);
            
            $log->info("kid is $kid ; backupPid is $backupPid");
            
            if ( $kid == $backupPid or $kid == -1 ){
                $log->info("backup child process $kid end");
                
                if ( $isDone == 0 ){
                    $log->warn("child process is killed");
                }
                
                last;
            }
            
            # 检查flush tables线程状态
            my $procStat = $self->{dbbackupObj}->runCommand("$mysqlDefaultConn -e 'show processlist'|egrep -iE 'Flush tables' |awk '{print \$1,\$6}'|sort -r |head -1");
            
            # 获取flush tables线程编号和锁定时间
            my ( $sessionNo,$tableLock ) = split(/\s+/,$procStat);
            if ( ! defined($tableLock) ){
                $tableLock = 0;
            }
            if ( !defined($sessionNo) ){
                $sessionNo = 0;
            }

            $log->info("processlist status is $procStat, session_no: $sessionNo, table lock time: $sessionNo");
            
            # 锁定时间大于预设值,则杀掉备份线程
            if ( ($tableLock+60) > $flushTableLockLimit ){
                $log->info("flush table lock longer than $flushTableLockLimit");
                $log->info("kill child process");
                
                $self->{dbbackupObj}->runCommand("pkill -TERM -P $backupPid");
                $self->{dbbackupObj}->runCommand("$mysqlDefaultConn -e 'kill $sessionNo'");
                
                # 备份被阻塞
                $isBlocked = 1;
                
                last;
            }
        }
        
        # 备份成功
        if ( $isDone ){
            last;
        }
        
        # 恢复服务器timeout时间值
        $self->{dbbackupObj}->runCommand("$mysqlDefaultConn -e 'set global interactive_timeout=$interactiveTimeout;set global wait_timeout=$waitTimeout'"); 
    }
    
    # 获取备份集大小
    if ( -d $bakDir ){
        $size = $self->{dbbackupObj}->runCommand("du -s $bakDir|awk '{print \$1}'");
    }else{
        $size = 0;
    }
    $log->info("backupset size: $size");

    $message = "";
    $backupsetStatus = "";
    $updateTime = $self->{dbbackupObj}->getCurrentTime();

    # xtabackup备份被阻塞情况
    if ( $isBlocked ){
        $message = "flush_table_timeout";
        $isSuccess = 0;
    }
    
    # xtrabackup备份被kill情况
    if ( $isKilled ){
        $message = "xtrabackup_been_killed";
        $isSuccess = 0;
    }
    
    # 检查备份日志
    $log->debug($isDone);
    $log->debug($detailLog);

    if ( $isDone ){
        if ( -f $detailLog ){
            my $cmd = "tail -n 3 $detailLog | grep -i -c 'completed OK!'";
            $log->debug($cmd);

            my ( $result,$exitCode ) = $self->{dbbackupObj}->runCommand($cmd);
            
            $log->debug($result);
            
            if ( $result > 0 ){
                # 备份成功
                $isSuccess = 1;
            }else{
                # 备份失败
                $isSuccess = 0;
                $log->error("found error in $detailLog"); 
            }
        } else {
            # 备份失败
            $isSuccess = 0;
            $log->error("$detailLog does not exist"); 
        } 
    }

    if ( $isDone and $isSuccess ){
        # xtrabackup备份成功
        $log->info("xtrabackup success");
        $message = "success";
        $backupsetStatus = "ok";
        
        # 获取binlog日志同步点
        if ( lc($isSlave) eq "n" ){
            # 备份节点为主库
            my $binlogInfo = "$bakDir/xtrabackup_binlog_info";
            if ( -f $binlogInfo ){
                $masterLogFile = $self->{dbbackupObj}->runCommand("awk '{print \$1}' $binlogInfo");
                $masterLogPos = $self->{dbbackupObj}->runCommand("awk '{print \$2}' $binlogInfo");
                $log->info("master node binlog info: $masterLogFile, $masterLogPos");
            }else{
                $log->error("$bakDir/xtrabackup_binlog_info does not exist");
            }
        } elsif ( lc($isSlave) eq "y" ){
            # 备份节点为从库
            my $binlogInfo = "$bakDir/xtrabackup_slave_info";
            if ( -f $binlogInfo ){
                $masterLogFile = $self->{dbbackupObj}->runCommand("awk -F \"'\" '{print \$2}' $binlogInfo");
                $masterLogPos = $self->{dbbackupObj}->runCommand("awk -F \"'\" '{print \$3}' $binlogInfo | awk -F \"=\" '{print \$2}'");
                $log->info("slave node binlog info: $masterLogFile, $masterLogPos");
            }else{
                $log->error("$bakDir/xtrabackup_slave_info does not exist");
            }
        }
    }else{
        # xtrabackup备份失败
        system("rm -rf $bakDir");
        $log->error("backup failed, remove backupset");
    }
    
    # 更新备份资料库
    $endTime = $self->{dbbackupObj}->getCurrentTime();
    $updateTime = $self->{dbbackupObj}->getCurrentTime();
    
    $self->{dbbackupObj}->updateBackupInfo($recordId,$endTime,$size,$status,$message,
        $fileDir,$backupsetStatus,$updateTime,$masterLogFile,$masterLogPos);   
    
    # 删除临时文件
    unlink $tmpBackupScript or $log->warn("remove $tmpBackupScript failed");
    unlink $statusFile or $log->warn("remove $statusFile failed");
    
    return $isSuccess;
}

1;
