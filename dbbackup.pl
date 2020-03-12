#!/usr/bin/env perl
# Description:  数据库备份主脚本

use strict;
use warnings;

use File::Spec;
use Log::Log4perl;
use Fcntl qw(:flock);
use POSIX qw(strftime);
use POSIX qw(:signal_h);

use FindBin qw($Bin);
use lib "$Bin/common";

use Dbconfig;
use Dbbackup;
use ManagerUtil;
use Xtrabackup;
use Binlog;
use Ftp;
use Mysqldump;
use Mylvm;
use Mongolvm;
use Mongodump;


# 获取脚本名称
my $scriptName = $0;
chomp($scriptName);

# 获取盘符、目录、文件名
my ( $scriptVolume, $scriptDir, $scriptFile ) = File::Spec->splitpath( $scriptName );

# 创建Dbconfig对象
my $dbconfigObj = new Dbconfig();
if ( !$dbconfigObj ){
    $dbconfigObj = new Dbconfig();
}

# 创建Dbbackup对象
my $dbbackupObj = new Dbbackup( dbconfigObj => $dbconfigObj );
if ( !$dbbackupObj ){
    $dbbackupObj = new Dbbackup( dbconfigObj => $dbconfigObj );
}

# 获取当前主机ip
my $localIp = $dbbackupObj->getIpAddr();
chomp($localIp);

# 获取当前时间
my $curTime = `date "+%Y-%m-%d %H:%M:%S"`;
chomp($curTime);

# 获取当前日期
my $curDate = `date "+%Y%m%d"`;
chomp($curDate);

# 获取当前星期
my $dayOfWeek = `LANG="en_US.UTF-8";date +%a`;
chomp($dayOfWeek);
$dayOfWeek = lc($dayOfWeek);

# 获取当前日期
my $dayOfMonth = `LANG="en_US.UTF-8";date +%d`;
chomp($dayOfMonth);
$dayOfMonth = lc($dayOfMonth);

# 获取脚本日志目录 
my $logDir = "/home/mysql/dbadmin/logs/" . $curDate;

# 设置日志文件
my $logFile;
if ( -e $logDir ){
    $logFile = "$logDir/$curDate" . "_" . $localIp . "_dbbackup.log";
}else{
    mkdir($logDir, 0755);
    $logFile = "$logDir/$curDate" . "_" . $localIp . "_dbbackup.log";
}

# 初始化log4perl
my $log = $dbbackupObj->initLog4Perl($logFile);


# 标识一次备份流程，方便查看日志
my $flag = int(rand(1000000));

$log->info("===============  begin($flag)  =================");


# 设置文件锁，防止脚本并行执行
my $lockFile = "/tmp/dbbackup_$flag.lock";
$log->info("start get lockfile");

my $flockStatus = getFileLock("$lockFile");
my $exitCode = $$flockStatus{'exitCode'};
if ( $exitCode != 0 ){
    # 获取文件锁失败，不发起备份
    $log->error("$scriptName is running, do not run again");
    $log->info("==  end($flag) ==");
    
    exit 0;
}

# 获取文件锁成功
$log->info("get lockfile: $lockFile success");

# 获取mytab配置文件
my $myTab = $dbconfigObj->get('mytab');

# 获取mysql默认连接信息


# 获取备份资料库数据库连接信息
my $repoHost = $dbconfigObj->get('repoHost');
my $repoPort = $dbconfigObj->get('repoPort');
my $repoUser = $dbconfigObj->get('repoUser');
my $repoPass = $dbconfigObj->get('repoPassword');
my $repoDb = $dbconfigObj->get('repoDb');

# 连接备份资料库
my $dbh = $dbbackupObj->mysqlConnect($repoHost,$repoPort,$repoDb,$repoUser,$repoPass);
if ( !$dbh ){
    $log->error("connect to $repoHost:$repoPort failed");
}
$log->info("connect to $repoHost:$repoPort success"); 

# 获取备份调度计划
my $sql = "select ip,port,bak_type,db_type,level,level_value,is_compressed,is_slave,parallel,retention,"
    . "is_encrypted,schedule_type,schedule_time,storage_ip,storage_type,lvm_expire_days,mysqldump_expire_days,"
    . "mongodump_expire_days,mysql_hotbak_expire_days,mysql_binlog_expire_days,ftp_expire_days,"
    . "lvm_speed,mysql_binlog_speed,mysql_hotbak_throttle"
    . " from backup_config where ip='$localIp'";

$log->info("get backup schedule config SQL:");
$log->info($sql);

# 执行查询操作
my $sth = $dbh->prepare($sql);

my $res = $sth->execute();

# 若结果为空, 则返回undef
if ( !$res ){
    $log->error("get $localIp backup schedule config from repo database failed");    
}
$log->info("get $localIp backup schedule config from repo database success");

# 遍历备份计划
while ( my $row = $sth->fetchrow_hashref()){

    # 设置能否发起备份标识
    my $runBackup = 0;
 
    chomp($row); 
    my $host = $row->{ip};
    my $port = $row->{port};
    my $bakType = $row->{bak_type};
    my $dbType = $row->{db_type};
    my $level = $row->{level};
    my $levelValue = $row->{level_value};
    my $isCompressed = $row->{is_compressed};
    my $isSlave = $row->{is_slave};
    my $parallel = $row->{parallel};
    my $retention = $row->{retention};
    my $isEncrypted = $row->{is_encrypted};
    my $scheduleType = $row->{schedule_type};
    my $scheduleTime = $row->{schedule_time};
    my $storageIp = $row->{storage_ip};
    my $storageType = $row->{storage_type};
    my $lvmExpireDays = $row->{lvm_expire_days};
    my $mysqldumpExpireDays = $row->{mysqldump_expire_days};
    my $mongodumpExpireDays = $row->{mongodump_expire_days};
    my $mysqlHotbakExpireDays = $row->{mysql_hotbak_expire_days};
    my $mysqlBinlogExpireDays = $row->{mysql_binlog_expire_days};
    my $ftpExpireDays = $row->{ftp_expire_days};
    my $lvmSpeed = $row->{lvm_speed};
    my $mysqlBinlogSpeed = $row->{mysql_binlog_speed};
    my $mysqlHotbakThrottle = $row->{mysql_hotbak_throttle};

    $log->info("get backup schedule config info:");
    $log->info("[host]:$host");
    $log->info("[port]:$port");
    $log->info("[db_type]:$dbType");
    $log->info("[bak_type]:$bakType");
    $log->info("[level]:$level");
    $log->info("[level_value]:$levelValue");
    $log->info("[is_compressed]:$isCompressed");
    $log->info("[is_slave]:$isSlave");
    $log->info("[parallel]:$parallel");
    $log->info("[retention]:$retention");
    $log->info("[is_encrypted]:$isEncrypted");
    $log->info("[schedule_type]:$scheduleType");
    $log->info("[schedule_time]:$scheduleTime");
    $log->info("[storage_ip]:$storageIp");
    $log->info("[storage_type]:$storageType");
    $log->info("[lvm_expire_days]:$lvmExpireDays");
    $log->info("[mysqldump_expire_days]:$mysqldumpExpireDays");
    $log->info("[mongodump_expire_days]:$mongodumpExpireDays");
    $log->info("[mysql_hotbak_expire_days]:$mysqlHotbakExpireDays");
    $log->info("[mysql_binlog_expire_days]:$mysqlBinlogExpireDays");
    $log->info("[ftp_expire_days]:$ftpExpireDays");
    $log->info("[lvm_speed]:$lvmSpeed");
    $log->info("[mysql_binlog_speed]:$mysqlBinlogSpeed");
    $log->info("[mysql_hotbak_throttle]:$mysqlHotbakThrottle");

    # 检查判断能否发起备份
    $log->info("check instance $host:$port can run backup or not");
    
    if ( lc($scheduleType) =~ /week/ ){
        if ( lc($scheduleTime) =~ /$dayOfWeek/ ){
            # 可发起备份
            
            $log->info("instance $host:$port today can run backup, backup type: $bakType");

            $runBackup = 1;            
        }
    } elsif ( lc($scheduleType) =~ /month/ ){
        if ( lc($scheduleTime) =~ /$dayOfMonth/ ){
            # 可发起备份
            
            $log->info("instance $host:$port can run backup today, backup type is: $bakType");
            
            $runBackup = 1;
        }
    }

    # 0表示不备份error logs和slow logs
    my $runBackupLogs = 0;

    # 获取备份存储目录
    my $storageDir = $dbbackupObj->getStorageDir( $host,$port,$dbType,$storageType );
    if ( ! $storageDir ){
        $log->error("get storage dir failed");
    } else{
        $log->info("get storage dir success");
        $log->info("storage dir: $storageDir");        
    }
    
    # 发起备份
    if ( $runBackup == 1 ){
        
        eval{
            if ( lc($bakType) =~ /mysqldump/ ){
                # 发起mysqldump备份
                my $mysqldumpObj = new Mysqldump( dbconfigObj => $dbconfigObj, dbbackupObj => $dbbackupObj );
                
                $mysqldumpObj->main($host,$port,$dbType,$isSlave,$bakType,$level,$levelValue,$curDate,
                    $isEncrypted,$isCompressed,$retention,$mysqldumpExpireDays,$storageType);
    
                # 备份error logs和slow logs
                if ( $runBackupLogs == 1 ){                                 
                    $log->info("backup error and slow logs to storage");
                    $dbbackupObj->backupLogs($port,$storageDir);
                }
                
            } elsif ( lc($bakType) =~ /xtrabackup/ ){
                # 发起hotbak备份
                my $xtrabackupObj = new Xtrabackup( dbconfigObj => $dbconfigObj, dbbackupObj => $dbbackupObj );         
    
                $xtrabackupObj->main($host,$port,$dbType,$isSlave,$bakType,$level,$levelValue,$curDate,
                    $isEncrypted,$isCompressed,$retention,$mysqlHotbakExpireDays,
                    $mysqlHotbakThrottle,$storageType);
    
                # 备份error logs和slow logs
                if ( $runBackupLogs == 1 ){
                    $log->info("backup error and slow logs to storage");
                    $dbbackupObj->backupLogs($port,$storageDir);
                }
    
            } elsif ( lc($bakType) =~ /ftp/ ){
                # 发起ftp备份
                my $ftpObj = new Ftp( dbconfigObj => $dbconfigObj, dbbackupObj => $dbbackupObj );
                
                $ftpObj->main($host,$port,$dbType,$isSlave,$bakType,$level,$levelValue,$curDate,
                    $isEncrypted,$isCompressed,$retention,$storageType);
    
            } elsif ( lc($bakType) =~ /mylvm/ ){
                # 发起mylvm备份
                my $lvmObj = new Mylvm( dbconfigObj => $dbconfigObj, dbbackupObj => $dbbackupObj );
                
                $lvmObj->main($host,$port,$dbType,$isSlave,$bakType,$level,$levelValue,$curDate,
                $isEncrypted,$isCompressed,$retention,$storageType,$lvmExpireDays,$lvmSpeed);
                          
            } elsif ( lc($bakType) =~ /binlog/ ){
                # 发起binlog备份
                my $binlogObj = new Binlog( dbconfigObj => $dbconfigObj, dbbackupObj => $dbbackupObj );
                
                $binlogObj->main($host,$port,$dbType,$isSlave,$bakType,$level,$levelValue,$curDate,
                    $isEncrypted,$isCompressed,$retention,$storageType,
                    $mysqlBinlogExpireDays,$mysqlBinlogSpeed);
                     
            } elsif ( lc($bakType) =~ /mongolvm/ ){
                # 发起mongolvm备份
                my $mongolvmObj = new Mongolvm( dbconfigObj => $dbconfigObj, dbbackupObj => $dbbackupObj );
                
                $mongolvmObj->main($host,$port,$dbType,$isSlave,$bakType,$level,$levelValue,$curDate,
                $isEncrypted,$isCompressed,$retention,$storageType,$lvmExpireDays,$lvmSpeed); 
    
            } elsif ( lc($bakType) =~ /mongodump/ ){
                # 发起mongodump备份
                my $mongodumpObj = new Mongodump( dbconfigObj => $dbconfigObj, dbbackupObj => $dbbackupObj );
                
                $mongodumpObj->main($host,$port,$dbType,$isSlave,$bakType,$level,$levelValue,$curDate,
                $isEncrypted,$isCompressed,$retention,$mongodumpExpireDays,$storageType);
       
            } else {
                $log->error("not support backup type: $bakType");
                
                next;
            }
        };
        if ($@){
            $log->error("something error in backup, host:$host,port:$port,bakType:$bakType, error msg: $@");
        }
    } else {
        $log->info("$host:$port $bakType have no backup schedule today");

        next;
    }
}
$sth->finish();


# 删除文件锁
$log->info("start removing $lockFile");

my $delFileLock = cleanFileLock($lockFile);
if ( !$delFileLock ){
    $log->error("remove $lockFile failed");
}else{
    $log->info("remove $lockFile success");
}

# 结束
$log->info("================  end($flag)    =======================");
