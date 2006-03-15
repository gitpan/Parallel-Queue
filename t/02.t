
########################################################################
# test whether the debug-mode dispatcher works properly (via jobcount
# of zero).
########################################################################

use strict;
use FindBin::libs;

use Test::More qw( tests 7 );

use Parallel::Queue qw( runqueue debug );

########################################################################
# first sanity check: was the 'runqueue' function installed?

ok( __PACKAGE__->can( 'runqueue' ), 'runqueue installed' );

########################################################################
# process creates and removes some files in parallel.

my $tmp = "./$$";

my @filz = map { "$tmp/$_" } ( 'aa' .. 'at' );

my @pass_1
= map
{
    my $path = $_;
    sub { open my $fh, '>', $path; 0 }
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
# run in debug mode without forking/threading off
# the individual jobs.

for my $i ( 0, 1, 4 )
{
    runqueue $i, @pass_1;

    my $result_1 = grep { -e } @filz;

    runqueue $i, @pass_2;

    my $result_2 = grep { -e } @filz;

    ok( $result_1 == @filz  , "Pass $i files created" );

    ok( $result_2 == 0      , "Pass $i files deleted" );
}

########################################################################
# cleanup on the way out

eval
{
    unlink @filz;

    rmdir $tmp;
};

__END__
