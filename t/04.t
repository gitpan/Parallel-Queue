########################################################################
# test using runqueue as a class method -- also tests large job
# lists.
########################################################################

use strict;
use FindBin::libs;

use Test::More qw( tests 3 );

use Parallel::Queue;

########################################################################
# create and drop a large  list of files in 
# interleaved groups of three. If the queue is dropping jobs 
# then this should show it.

my $tmp = "./$$";

my @filz = map { "$tmp/$_" } ( 'aa' .. 'zz' );

# cleanup may fail on the way in.

-d $tmp || mkdir $tmp, 0700
or die "Roadkill: $tmp: $!";

eval { unlink glob "$tmp/*" };

# create a few files, then a few more, then delete the
# first group, then create more, then delete... this 
# is a decent test of whether the queue is dropping jobs.

sub frobnicate
{
    -e $_[0]
    ? unlink $_[0]
    : open my $fh, '>', $_[0];

    0
}

my @queue = map { my $a = $_ ; sub{ frobnicate $a } } @filz[0..2];

my $i = @filz;

for( 1 .. 2 * $i - 3 )
{
    push @queue,
    map
    {
        my $b = $_;
        sub { frobnicate $b }
    }
    grep
    {
        defined
    }
    @filz[3..5,0..2];

    splice @filz, 0, 3;
}

for my $i ( 0, 1, 8 )
{
    Parallel::Queue->runqueue( 8, @queue );

    my $count = scalar ( my @a = glob "$tmp/*" );

    print "Leftover files: $count\n" if $count;

    ok( $count == 0, "Pass $i: No leftover files" );
}

########################################################################
# cleanup on the way out

eval { rmdir $tmp };

__END__
