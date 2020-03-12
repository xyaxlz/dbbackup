# Description: file manager

package ManagerUtil;

use strict;
use warnings;

use Log::Log4perl;
use Fcntl qw(:flock);
use Exporter ();
use Tie::File;

our @ISA = qw(Exporter);
our @EXPORT = qw( getFileLock cleanFileLock modifyGotoFault getIpAddr compareIp ); 
our @VERSION = 1.0;


# @Description:  加文件锁
# @Param: 
# @Return: 
sub getFileLock{    
    my ($lock_file) = @_;

    my $log = Log::Log4perl->get_logger(""); 
    my %flock_status = ();
            
    eval {
        if ( -e $lock_file ){
            open (FD, " < $lock_file") or do {
                $log->error("$lock_file has exist, but can not open it,$!");
                exit 0;
            };
            
            if( flock(FD, LOCK_EX | LOCK_NB) ){
                $flock_status{'exitCode'} = 0;
            }else{
                $flock_status{'exitCode'} = 1;
            }
        } else {
            open (FD, " > $lock_file") or do {
                $log->error("Can not create $lock_file,$!");
                exit 0;
            };
            if( flock(FD, LOCK_EX | LOCK_NB) ){
                $flock_status{'exitCode'} = 0;
            }else{
                $flock_status{'exitCode'} = 1;
            }               
        }
    };
    if ($@){
        undef $@;
    }

    return (\%flock_status);
}

# @Description:  删除文件锁
# @Param: 
# @Return: 
sub cleanFileLock{
    my $lock = @_;
    
    my $log = Log::Log4perl->get_logger("");
     
    if ($lock) {
        eval {
            system("rm -f @_");
        };
        if ($@){
            $log->error("del filelock failed");
            undef $@;
            
            return;
        }      
    }
    
    return 1;
}

# @Description: 修改haconf的gotoFault值
# @Param: 端口 vrrp实例名 vip gotofault值
# @Return:  1:success  0:failed
sub modifyGotoFault {
    my ($gPort, $gVrrginstanceName, $gVip, $gGotoFault) = @_;
    
    my $log = Log::Log4perl->get_logger("");
    
    my @haconf;
    my $file = "/etc/keepalived/haconf";
    my $time = getLocaltime();
    my $filebak = $file .".bak.".$time .int(rand(10000));
    if (!defined($gPort) && !defined($gVrrginstanceName) && !defined($gVip) && !defined($gGotoFault)){
        $log->error("argument error");
        return 0; 
    } 
    eval { 
        tie @haconf, 'Tie::File', $file or die "CAN_NOT write array haconf"; 
        open HACONFBAK, ">$filebak" or die "CAN_NOT write $filebak";
        foreach my $line (@haconf) {
            chomp($line);
            # 备份
            print HACONFBAK $line . "\n";
            # 字符串转换为数组
            my @temp = (split (/\s+/, $line));
            # 查找匹配的行
            if ($gPort eq $temp[3] && lc($gVrrginstanceName) eq lc($temp[7]) && $gVip eq $temp[8]){
                if ($gGotoFault) {
                    if (lc($gGotoFault) eq lc($temp[6])){        
                        $log->info("$gVrrginstanceName:$gVip:$gPort original gotoFault=$gGotoFault, no need modify");
                    }else{
                        $temp[6] = uc($gGotoFault);
                        # 数组转换为字符串保存
                        $line = join('    ',@temp);
                        $log->info("$gVrrginstanceName:$gVip:$gPort modify gotoFault=$gGotoFault success");
                    }
                }
            }else{
                $log->debug("line not match $gVrrginstanceName:$gVip:$gPort"); 
            }
        }
        close HACONFBAK;
        untie(@haconf);
    };
    if ($@) {
        if ($@ =~ /CAN_NOT/ ) {
            # 若上面的tie或者open操作发生die，则修改失败
            $log->error("modify haconf gotoFault error");
            
            return 0;
        }
        undef $@;        
    }
    
    return 1;
}

sub getLocaltime {
    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time());
    $year = $year + 1900;
    $mon++; 
    my $localtime = $year . $mon . $mday . $hour . $min . $sec;
    
    return $localtime;
}

# @Description:  取ip地址
# @Param: 
# @Return: 返回ip地址
sub getIpAddr {
    my $ipAddr;

    eval {
        $ipAddr = `hostname -i`;
        chomp($ipAddr);
        $ipAddr =~ s/^\s+//g;
    };
    if($@){
        undef $@; 
    }
    
    return $ipAddr;
}

# @Description:  比较两个ip大小
# @Param: $ip1 $ip2
# @Return: 若$ip1 > $ip2，则返回1；否则，返回0
sub compareIp {
    my ($ip1,$ip2) = @_;
   
    my @a = split(/\./, $ip1);
    my @b = split(/\./, $ip2);
   
    for ( my $i=0; $i<4; $i++ ){
        if ( $a[$i] == $b[$i] ){
            next;
        } elsif ( $a[$i] > $b[$i] ) {
            return 1;
        } elsif ( $a[$i] < $b[$i] ) {
            return 0;
        }
    }
}

1;

