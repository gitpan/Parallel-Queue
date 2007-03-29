########################################################################
# test job failure
########################################################################

use strict;

use Test::More qw( tests 3 );

use Parallel::Queue qw( runqueue );

my $pass_1
= do
{
    my @queue =
    (
        sub { 0 },
        sub { 0 },

        sub { 1 },  # non-zero exit == failure.

        sub { 0 },
        sub { 0 },
    );

    # there isn't any good way to test this with more
    # than one job running due to logic races at the 
    # O/S level.

    runqueue 1, @queue;
};

ok( 2 == $pass_1, 'Two jobs left over' );

my @pass_2
= do
{
    # the spin loop allows anyone watching via, say,
    # top to see that the processes are actually being
    # forked...

    my $sub
    = sub
    {
        my $i = $_ + 1 for (1..1_000_000);

        0
    };

    my @queue =
    (
        ( $sub ) x 20,

        sub { 1 },  # non-zero exit == failure.

        ( $sub ) x 20,
    );

    # can't tell in advance how many jobs are incomplete
    # but there should be at least some of them...

    runqueue 8, @queue
};

ok( @pass_2, "Parallel jobs remaining after failure" );

my @pass_3 = runqueue 8, @pass_2;

ok( ! @pass_3, "Remaining pass-2 jobs completed" );

__END__
