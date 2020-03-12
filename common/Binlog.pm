# Description:  backup mysql binlog

package Binlog;

use strict;
use warnings;

use File::Path;
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
        $isEncrypted,$isCompressed,$retention,$storageType,
        $mysqlBinlogExpireDays,$mysqlBinlogSpeed ) = @_;

    my $log = Log::Log4perl->get_logger("");
    
    # 实例备份目录
    my $baseDir = $self->{dbbackupObj}->getStorageDir($host,$port,$dbType,$storageType);
    if ( ! $baseDir ){
        $log->error("storage dir is not mounted, stop backup");
        return 0;
    }
    $log->debug("get \$baseDir: $baseDir");

    my $fileDir = "binlog/$backupDate\_$host\_$port";
    $log->debug("get \$fileDir: $fileDir");
    
    my $bakDir = "$baseDir/$fileDir";
    $log->debug("get \$bakDir: $bakDir");

    # bin log文件名
    my $binlogName = "mysql-bin";

    # 检查备份
    my $isExistBackupset = $self->{dbbackupObj}->isExistBackupset($host,$port,$dbType,$bakType,$bakDir,$fileDir);
    chomp($isExistBackupset);
    $log->debug("get \$isExistBackupset: $isExistBackupset");

    if ( $isExistBackupset == 0 ){
    
        my $total = 0;      # 备份总数
        my $success = 0;    # 备份成功次数
        my $failed = 0;     # 备份失败次数
        my $skip = 0;       # 备份跳过次数
        my $startTime = $self->{dbbackupObj}->getCurrentTime();   # 备份开始时间
        my $endTime = "0000-00-00 00:00:00";
        my $size = 0;
        my $status = 0;
        my $message = "running";
        my $backupsetStatus = "";
        my $updateTime = $self->{dbbackupObj}->getCurrentTime();
        my $masterLogFile = "no";
        my $masterLogPos = 0;

        # 创建备份目录
        system("mkdir -p $bakDir");
    
        # 获取filer存储目录
        my $filerDir = $self->{dbbackupObj}->getFilerDir($host,$port,$dbType,$storageType);
        $log->info("get filerDir is: $filerDir");
    
        # 更新备份资料库
        my $recordId = $self->{dbbackupObj}->insertBackupInfo($host,$port,$dbType,$bakType,$level,
            $levelValue,$startTime,$endTime,$size,$status,$message,$filerDir,$fileDir,$backupsetStatus,
            $updateTime,$isSlave,$isCompressed,$isEncrypted,$masterLogFile,$masterLogPos);
    
        $log->info("get recordId is $recordId");
    
        # 获取最后一次备份成功的binlog
        my $latestFile;
 
        my $latestBakBinlog = $self->getLatestBakBinlog($host,$port,$bakType);
        if ( defined($latestBakBinlog) ){
            $log->info("get latest bak binlog: $latestBakBinlog");
           
            if ( $latestBakBinlog ) {
                $latestFile = (split(",",$latestBakBinlog))[1];
            }
            
            if ( !$latestFile ){
                $latestFile = "0.0";
            }
            $log->info("get latest bak binlog file: $latestFile");
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
    
        # 获取binlog文件列表
        my $binlogDir = $self->getBinlogDir($defaultHost,$defaultPort,$defaultDbname,
            $defaultDbuser,$defaultDbpass);
        
        $log->info("get binlogDir: $binlogDir");
        
        my $curFile = $self->{dbbackupObj}->getMysqlVariableValue($dbh,"show master status");
        
        # 执行flush logs
        $self->{dbbackupObj}->mysqlExecuteOnly($dbh,"flush logs");
        $log->info("$host:$port execute flush logs success");
    
        open BINLOGLIST,$binlogDir.("/").$binlogName.(".index");
        
        # 逐个拷贝binlog文件到备份目录
        while ( my $file = <BINLOGLIST> ){
            chomp($file);
            $log->debug("\$file: $file");
            my $index = `echo $file |awk -F"$binlogName" '{print \$2}'`;
            $log->debug("\$index is $index");
            
            $index =~ s/\.//g;
            chomp($index);
            $log->debug("\$index is $index");
            
            my $indexInt = int($index);
            $log->debug("\$indexInt is $indexInt");
            
            my $latestFileInt = int((split('\.',$latestFile))[1]);
            $log->debug("\$latestFileInt is $latestFileInt");
            
            if ( $indexInt > $latestFileInt ){
                my $binlogFile = "$binlogDir/$binlogName.$index";
                chomp($binlogFile);
                $log->debug("\$binlogFile is $binlogFile");
                
                if ( ! -e "$binlogFile" ){
                    $log->warn("binlog file: $binlogFile not exist, skip it");
                    $skip++;
                    
                    next;
                }
                
                # rsync传输数据
                my $result = $self->rsyncData($binlogFile,$bakDir,$mysqlBinlogSpeed);
                if ( !$result ){
                    $failed++;
                    $log->error("rsync binlog file: $binlogName.$index failed");
                    $log->error("failed: $binlogFile");
                    
                    last;
                }
                
                $total++;
            }
        }
        close BINLOGLIST;
      
        if ( $failed > 0 ){
            $success = 0;
            $log->error("backup binlog file numbers: $total, binlog file numbers: $failed");
        } else {
            $success = 1;
            $log->info("backup binlog file numbers: $total, skip binlog file numbers: $skip");
        }
        
        # 更新备份资料库
        if ( $success == 1 ){
            # 备份文件大小
            $size = $self->{dbbackupObj}->runCommand("du -s $bakDir |awk '{print \$1}'");
            
            # 备份状态信息
            $message = "success";
            
            # 备份集状态
            $backupsetStatus = "ok";
            
            # 最后一次备份binlog文件
            $masterLogFile = "$latestFile,$curFile";
        }
        
        # 备份结束时间
        $endTime = $self->{dbbackupObj}->getCurrentTime();
        
        # 更新时间
        $updateTime = $self->{dbbackupObj}->getCurrentTime();
        $masterLogPos = 0;
        
        # 更新备份资料库
        $self->{dbbackupObj}->updateBackupInfo($recordId,$endTime,$size,$success,$message,
            $fileDir,$backupsetStatus,$updateTime,$masterLogFile,$masterLogPos);
    
        if ( $success ){
            return 1;
        }
        
        return 0;
    } else {
        $log->info("instance $host:$port backupset exists, stop backup");
    }
}

# @Description: 检查备份目录
# @Param: 
# @Return: 
sub checkBakDir {
    my ( $self,$bakDir ) = @_;
    
    my $log = Log::Log4perl->get_logger("");
    
    # 判断备份目录是否存在
    if ( -d $bakDir ){
        # 若存在,则检查备份目录是否为空
        my $check = $self->isEmptyDir($bakDir);
        if ( $check ){
            $log->info("backup dir: $bakDir exists and is empty");
        }else{
            $log->error("backup dir: $bakDir exists and is not empty");
        }
    }else{
        # 创建备份目录
        unless (mkdir $bakDir){
            $log->warn("create $bakDir failed");
        }
    }
}

# @Description:  检查备份存储filer目录
# @Param: 
# @Return:  1:success  0:failed
sub checkFiler {
    my ( $self,$host,$port,$dbType,$storageType ) = @_;
    
    my $log = Log::Log4perl->get_logger("");
    
    # 若存在,则检查备份目录是否为空
    my $filer = $self->{dbbackupObj}->getStorageDir($host,$port,$dbType,$storageType);
    if ( $filer ){
        $log->info("filer is ok");
        
        return 1;
    }else{
        $log->error("filer is not ok");
        
        return 0;
    }
}

# @Description: 检查bin-log开启情况
# @Param: 
# @Return: 1:success  0:failed
sub isEnableLogBin {
    my ( $self,$host,$port,$dbname,$user,$pass ) = @_;
    
    my $log = Log::Log4perl->get_logger("");
    
    my $dbh = $self->{dbbackupObj}->mysqlConnect($host,$port,$dbname,$user,$pass);
    
    my $sql = "show variables like 'log_bin'";
    my $logBin = $self->{dbbackupObj}->getMysqlVariableValue($dbh,$sql);
    if ( lc($logBin) ne "on" or $logBin == 0 ){
        $log->info("log-bin is on");
        
        return 1;
    }else{
        $log->error("log-bin is disable");
        
        return 0;
    }
}

# @Description:  获取binlog目录
# @Param:  $host,$port,$dbname,$user,$pass
# @Return:  $binlogDir or 0
sub getBinlogDir {
    my ( $self,$host,$port,$dbname,$user,$pass ) = @_;
    
    my $log = Log::Log4perl->get_logger("");
    
    # 连接数据库
    my $dbh = $self->{dbbackupObj}->mysqlConnect($host,$port,$dbname,$user,$pass);

    my $sql = "show master status";

    # 检查有无binlog
    my $curBinlog = $self->{dbbackupObj}->getMysqlVariableValue($dbh,$sql);
    if ( !$curBinlog ){
        
        # binlog未开启
        $log->error("bin log is disable");
        
        return 0;
    }else{
        $log->info("get bin log info: $curBinlog");
    }
    
    # 获取数据目录
    my $dataDir = $self->{dbbackupObj}->getMysqlVariableValue($dbh,"select \@\@datadir");
    
    # binlog目录
    my $binlogDir = $dataDir;
    if ( -d $binlogDir ){
        
        return $binlogDir;
    }else{
        
        return 0;
    }
}

# @Description:  执行rsync
# @Param:  $source,$target,$speed
# @Return:  1:success  0:failed
sub rsyncData {
    my ( $self,$source,$target,$speed ) = @_;

    my $log = Log::Log4perl->get_logger("");
    
    # 退出码
    my $exitCode = 0;
    
    eval{
        my $cmd = "rsync -av $source $target --bwlimit=$speed";
        $log->info("rsync command: $cmd");
        
        # 执行rsync传输数据
        $log->info("starting rsync binlog data");
        
        my $result = `$cmd`;
        if ( $result =~ /error/ ){
            $log->error("rsync $source failed");
        }else{
            $exitCode = 1;
            $log->info("rsync $source success");
        }
        
        $log->info("finish rsync");
    };
    # 异常情况
    if ( $@ ){
        $log->error("rsync error: $@");
        undef $@;
    }
    
    return $exitCode;
}

# @Description:  压缩备份文件
# @Param:  
# @Return: 
sub compressBinlogFile {
    my ( $self ) = @_;
    
    my $log = Log::Log4perl->get_logger("");
}

# @Description:  获取资料库中上一次成功备份的binlog文件
# @Param:  
# @Return:  $latestBakBinlog or undef
sub getLatestBakBinlog {    
    my ( $self,$host,$port,$bakType ) = @_;
    
    my $log = Log::Log4perl->get_logger("");
        
    # 获取备份资料库数据库连接参数
    my ($repoHost,$repoPort,$repoUser,$repoPass,$repoDb) = $self->{dbbackupObj}->getRepoInfo();
    
    # 连接备份资料库
    my $dbh = $self->{dbbackupObj}->mysqlConnect($repoHost,$repoPort,$repoDb,$repoUser,$repoPass);
    if ( !$dbh ){
        $log->error("connect to $repoHost:$repoPort failed");
        
        # 连接mysql异常，返回undef
        return undef;
    }
    $log->info("connect to $repoHost:$repoPort success"); 
    
    # SQL语句
    my $sql = "select master_log_file from backup_info"
        . " where ip='$host' and port=$port and bak_type='$bakType' and status=1"
        . " order by id desc limit 1";
            
    $log->info($sql);
    
    # 执行查询操作
    my $latestBakBinlog = $self->{dbbackupObj}->mysqlQuery($dbh,$sql);
    if ( !$latestBakBinlog ){
        $log->error("get latest backup binlog failed");
        
        # 查询结果为空，返回undef
        return undef;
    }
    
    $log->info("get latest backup binlog success, binlog: $latestBakBinlog");
        
    return $latestBakBinlog->{master_log_file};
}

# @Description:  检查目录是否为空
# @Param:  $dir
# @Return:  1:目录为空  0:目录不为空
sub isEmptyDir {
    my ( $self,$dir ) = @_;
    
    my $log = Log::Log4perl->get_logger("");
    
    my $exitCode = 0;
    
    opendir(DIR, $dir) or $log->error("dir: $dir does not exists");
    
    if ( scalar(grep { $_ ne "." && $_ ne ".." } readdir(DIR)) == 0 ){
        # 目录为空
        $log->info("dir: $dir is empty");
        
        $exitCode = 1;
    }
    closedir(DIR);
    
    $log->warn("dir: $dir is not empty");
    
    return $exitCode;
}

1;
