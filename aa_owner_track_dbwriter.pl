# install dependencies:
#  sudo apt install cpanminus libjson-xs-perl libjson-perl libmysqlclient-dev libdbi-perl
#  sudo cpanm Net::WebSocket::Server
#  sudo cpanm DBD::MariaDB

use strict;
use warnings;
use JSON;
use Getopt::Long;
use DBI;
use Time::HiRes qw (time);
use Time::Local 'timegm_nocheck';

use Net::WebSocket::Server;
use Protocol::WebSocket::Frame;

$Protocol::WebSocket::Frame::MAX_PAYLOAD_SIZE = 100*1024*1024;
$Protocol::WebSocket::Frame::MAX_FRAGMENTS_AMOUNT = 102400;

$| = 1;

my $end_block_infinity = 5000000000;                         
my $end_date_infinity = '2100-01-01';

my $network;
my $contract = 'atomicassets';

my $port = 8800;

my $dsn = 'DBI:MariaDB:database=aa_owner_track;host=localhost';
my $db_user = 'aa_owner_track';
my $db_password = 'Naul1iofae';
my $commit_every = 100;
my $endblock = 2**32 - 1;


my $ok = GetOptions
    ('network=s' => \$network,
     'port=i'    => \$port,
     'ack=i'     => \$commit_every,
     'endblock=i'  => \$endblock,
     'dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
     'contract=s' => \$contract,
    );


if( not $network or not $ok or scalar(@ARGV) > 0 )
{
    print STDERR "Usage: $0 --network=X [options...]\n",
        "Options:\n",
        "  --network=X        network name\n",
        "  --port=N           \[$port\] TCP port to listen to websocket connection\n",
        "  --ack=N            \[$commit_every\] Send acknowledgements every N blocks\n",
        "  --endblock=N       \[$endblock\] Stop before given block\n",
        "  --dsn=DSN          \[$dsn\]\n",
        "  --dbuser=USER      \[$db_user\]\n",
        "  --dbpw=PASSWORD    \[$db_password\]\n";
    exit 1;
}


my $dbh = DBI->connect($dsn, $db_user, $db_password,
                       {'RaiseError' => 1, AutoCommit => 0,
                        mariadb_server_prepare => 1});
die($DBI::errstr) unless $dbh;



my $sth_upd_sync = $dbh->prepare
    ('INSERT INTO SYNC (network, block_num) VALUES(?,?) ' .
     'ON DUPLICATE KEY UPDATE block_num=?');


my $sth_end_ownership = $dbh->prepare
    ('UPDATE ' . $network . '_ASSET_OWNERSHIP SET end_block_num=?, end_block_time=? ' .
     'WHERE asset_id=? and end_block_num=' . $end_block_infinity);

my $sth_get_owner = $dbh->prepare
    ('SELECT owner FROM ' . $network . '_ASSET_OWNERSHIP WHERE asset_id=? and end_block_num=?');

my $sth_extend_ownership = $dbh->prepare
    ('UPDATE ' . $network . '_ASSET_OWNERSHIP SET end_block_num=' . $end_block_infinity . ', ' .
     'end_block_time=\'' . $end_date_infinity . '\' ' .
     'WHERE asset_id=? and end_block_num=?');

my $sth_set_ownership = $dbh->prepare
    ('INSERT INTO ' . $network . '_ASSET_OWNERSHIP (' .
     'asset_id, collection_name, schema_name, template_id, owner, start_block_num, start_block_time, ' .
     'end_block_num, end_block_time) ' .
     'VALUES (?,?,?,?,?,?,?,' . $end_block_infinity . ',\'' . $end_date_infinity . '\')' .
     'ON DUPLICATE KEY UPDATE owner=?');



my $committed_block = 0;
my $stored_block = 0;
my $uncommitted_block = 0;
{
    my $sth = $dbh->prepare
        ('SELECT block_num FROM SYNC WHERE network=?');
    $sth->execute($network);
    my $r = $sth->fetchall_arrayref();
    if( scalar(@{$r}) > 0 )
    {
        $stored_block = $r->[0][0];
        printf STDERR ("Starting from stored_block=%d\n", $stored_block);
    }
}


my $json = JSON->new;

my $blocks_counter = 0;
my $add_counter = 0;
my $del_counter = 0;
my $counter_start = time();

Net::WebSocket::Server->new(
    listen => $port,
    on_connect => sub {
        my ($serv, $conn) = @_;
        $conn->on(
            'binary' => sub {
                my ($conn, $msg) = @_;
                my ($msgtype, $opts, $js) = unpack('VVa*', $msg);
                my $data = eval {$json->decode($js)};
                if( $@ )
                {
                    print STDERR $@, "\n\n";
                    print STDERR $js, "\n";
                    exit;
                }

                my $ack = process_data($msgtype, $data);
                if( $ack >= 0 )
                {
                    $conn->send_binary(sprintf("%d", $ack));
                    print STDERR "ack $ack\n";
                }

                if( $ack >= $endblock )
                {
                    print STDERR "Reached end block\n";
                    exit(0);
                }
            },
            'disconnect' => sub {
                my ($conn, $code) = @_;
                print STDERR "Disconnected: $code\n";
                $dbh->rollback();
                $committed_block = 0;
                $uncommitted_block = 0;
            },

            );
    },
    )->start;


sub process_data
{
    my $msgtype = shift;
    my $data = shift;

    if( $msgtype == 1001 ) # CHRONICLE_MSGTYPE_FORK
    {
        my $block_num = $data->{'block_num'};
        print STDERR "fork at $block_num\n";
        $uncommitted_block = 0;
        return $block_num-1;
    }
    elsif( $msgtype == 1007 ) # CHRONICLE_MSGTYPE_TBL_ROW
    {
        my $block_num = $data->{'block_num'};
        if( $block_num > $stored_block )
        {
            my $kvo = $data->{'kvo'};
            if( $kvo->{'code'} eq $contract and $kvo->{'table'} eq 'assets' )
            {
                my $block_time = $data->{'block_timestamp'};
                $block_time =~ s/T/ /;
                my $val = $kvo->{'value'};
                my $asset_id = $val->{'asset_id'};

                if( $data->{'added'} eq 'false' )
                {
                    $sth_end_ownership->execute($block_num,
                                                $block_time,
                                                $val->{'asset_id'});
                    $del_counter++;
                }
                else
                {
                    my $owner = $kvo->{'scope'};
                    
                    my $old_owner;
                    $sth_get_owner->execute($asset_id, $block_num);
                    my $r = $sth_get_owner->fetchall_arrayref();
                    if( scalar(@{$r}) > 0 )
                    {
                        $old_owner = $r->[0][0];
                    }
                    
                    if( defined($old_owner) and $old_owner eq $owner )
                    {
                        $sth_extend_ownership->execute($asset_id, $block_num);
                    }
                    else
                    {                    
                        $sth_set_ownership->execute($val->{'asset_id'},
                                                    $val->{'collection_name'},
                                                    $val->{'schema_name'},
                                                    $val->{'template_id'},
                                                    $owner,
                                                    $block_num,
                                                    $block_time,
                                                    $owner);
                    }
                    
                    $add_counter++;
                }
            }
        }
    }
    elsif( $msgtype == 1010 ) # CHRONICLE_MSGTYPE_BLOCK_COMPLETED
    {
        $blocks_counter++;
        $uncommitted_block = $data->{'block_num'};
        if( $uncommitted_block - $committed_block >= $commit_every or
            $uncommitted_block >= $endblock )
        {
            $committed_block = $uncommitted_block;

            my $gap = 0;
            {
                my ($year, $mon, $mday, $hour, $min, $sec, $msec) =
                    split(/[-:.T]/, $data->{'block_timestamp'});
                my $epoch = timegm_nocheck($sec, $min, $hour, $mday, $mon-1, $year);
                $gap = (time() - $epoch)/3600.0;
            }

            my $period = time() - $counter_start;
            printf STDERR ("blocks/s: %5.2f, add/block: %5.2f, del/block: %5.2f, changes/s: %5.2f, gap: %6.2fh, ",
                           $blocks_counter/$period, $add_counter/$blocks_counter, $del_counter/$blocks_counter,
                           ($add_counter + $del_counter)/$period,
                           $gap);
            $counter_start = time();
            $blocks_counter = 0;
            $add_counter = 0;
            $del_counter = 0;

            if( $uncommitted_block > $stored_block )
            {
                $sth_upd_sync->execute($network, $uncommitted_block, $uncommitted_block);
                $dbh->commit();
                $stored_block = $uncommitted_block;
            }
            return $committed_block;
        }
    }

    return -1;
}

