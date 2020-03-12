# Description:  数据库备份系统配置参数

package Dbconfig;

use strict;
use warnings;
use Log::Log4perl;


# @Description:  load config data
sub load {
    my $self = shift;
    $self->{defaultDbHost} = '127.0.0.1';             # 数据库主机
    $self->{defaultDbPort} = 'xxxx';                  # 数据库端口
    $self->{defaultDbName} = 'mysql';                 # 数据库名称
    $self->{defaultDbUser} = 'xxxx';              # 数据库用户名
    $self->{defaultDbPassword} = 'xxxx';     # 数据库密码
    $self->{dbbackupDir} = "/home/mysql/dbadmin/scripts/dbbackup/";
    $self->{mysqldumpSplitScript} = "/home/mysql/dbadmin/scripts/dbbackup/common/mysqldump_split.sh";
    $self->{mysqlLvmExpireDays} = 15;
    $self->{mysqldumpExpireDays} = 30;
    $self->{mysqlHotbakExpireDays} = 15;
    $self->{mysqlBinlogExpireDays} = 30;
    $self->{hotbakFlushTableLockLimit} = 300;
    $self->{dumpFlushTableLockLimit} = 300;
    $self->{mysqlHomedir} = "/home/mysql";
    $self->{mysqlBaseLogdir} = "/home/mysql/dbadmin/logs";
    $self->{mongodbBaseLogdir} = "/home/mongodb/dbadmin/logs";
    $self->{infinidbBaseLogdir} = "/home/infinidb/dbadmin/logs";
    $self->{mytab} = "/etc/mytab";
    $self->{excuteCmdTimeout} = 60;    # dbbackup.pm $timeout 
    
    # 备份限速
    $self->{mysqlLvmSpeed} = 20000;    # rsync --bwlimit= 20000 KBPS (20MB/S)
    $self->{backupBinlogSpeed} = 20000;    # rsync --bwlimit= 20000 KBPS (20MB/S)
    $self->{mysqlHotbakThrottle} = 30;
    
    # ftp server 
    $self->{ftpuser} = 'ftpbak';
    $self->{ftppass} = 'xxxx';
    
    # 备份资料库
    $self->{repoHost} = 'xxx';
    $self->{repoPort} = '3306';
    $self->{repoDb} = 'xxx';
    $self->{repoUser} = 'xxx';
    $self->{repoPassword} = 'xxx'; 
}

# @Description: 构造函数
# @Return: 对象
sub new {
    my ( $class, %args ) = @_;
    
    my $self = {};  # allocate new hash for object
    bless( $self, $class );  # statement object type
    
    # load config
    $self->load();
    
    return $self;
}

# @Description: 根据key获取value
# @Param: 键
# @Return: 值
sub get {
    my ( $self, $key ) = @_;
    return $self->{$key};
}

# @Description: 
# @Param: 
# @Return: 
sub set {
	my ( $self, $key, $value ) = @_;
	$self->{$key} = $value;
}

1;
