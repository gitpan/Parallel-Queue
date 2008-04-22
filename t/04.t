########################################################################
# test using runqueue as a class method -- also tests large job
# lists.
########################################################################

use strict;

use Test::More;

use Parallel::Queue;

########################################################################
# create and drop a list of files. if any jobs are being dropped
# out of the queue then it'll show up here.

# cleanup may fail on the way in.

my $tmp
= do
{
    my $t   = '.';

    for( 'tmp', $$ )
    {
        $t .= "/$_";

        -d $t || mkdir $t, 0777
        or die "Unable to mkdir $t: $!";
    }

    eval { unlink glob "$t/*" };

    $t
};

my $create  = sub { !( open my $fh, '>', $_[0]  ) };
my $remove  = sub { !( unlink $_[0]             ) };

sub gen_queue
{
    my $sub     = shift;

    my $base    = 'z';

    map
    {
        my $file  = $_->[1];

        sub { $sub->( $file ) }
    }
    sort
    {
        $a->[0] <=> $b->[0]
    }
    map
    {
        [ int rand 100, ++$base ]
    }
    ( 1 .. 20 )
}

my @countz  = ( 0, 1, 8, 100 );

plan tests => scalar @countz;

for my $count ( @countz )
{
    print "Pass: $count\n";

    for( $create, $remove )
    {
        my @queue   = gen_queue $_; 

        runqueue $count, @queue;
    }

    my $found   = glob "$tmp/*";

    ok ! $found, "Pass $count: No leftover files";
}

########################################################################
# cleanup on the way out

eval { rmdir "./tmp/$$"; rmdir './tmp'; };

__END__
