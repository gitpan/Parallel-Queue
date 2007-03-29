########################################################################
# test OO interface with debugging cycle.
########################################################################

use strict;

use Test::More qw( tests 8 );

use Parallel::Queue;

########################################################################
# process creates and removes some files in parallel.

my $tmp = "./$$";

my @filz = map { "$tmp/$_" } ( 'aa' .. 'at' );

# cleanup may fail on the way in.

my @pass_1
= map
{
    my $path = $_;
    sub { open my $fh, '>', $path or die "$path: $!"; 0 }
}
@filz;

my @pass_2
= map
{
    my $path = $_;
    sub { unlink $path or die "$path: $!"; 0 }
}
@filz;

########################################################################
# create a scratch directory and make sure that
# it is clean.

-d $tmp || mkdir $tmp, 0770
or die "Roadkill: '$0' unable to make '$tmp': $!";

eval { unlink @filz };

########################################################################
# first sanity checks: was the object created properly?

my $que_mgr = Parallel::Queue->construct( qw( debug fork ) );

ok( $que_mgr, 'Queue manager created' );
ok( $que_mgr->()->{ debug }, 'Queue manager is debugging' );

for my $i ( 0, 1, 8 )
{
    $que_mgr->runqueue( $i, @pass_1 );

    my $result_1 = grep { -e } @filz;

    $que_mgr->runqueue( $i, @pass_2 );

    my $result_2 = grep { -e } @filz;

    ok( $result_1 == @filz  , "Pass $i Files created" );

    ok( $result_2 == 0      , "Pass $i Files deleted" );
}

########################################################################
# cleanup on the way out

eval
{
    unlink @filz;

    rmdir $tmp;
};

__END__
