
########################################################################
# test whether the debug-mode dispatcher works properly (via jobcount
# of zero).
########################################################################

use v5.10;
use strict;

use Test::More;

use Symbol          qw( qualify_to_ref  );

use Parallel::Queue qw( fork );

ok __PACKAGE__->can( 'runqueue' ), "Installed 'runqueue'";

########################################################################
# create and remove files.

my $dir     = './tmp/';

-d $dir || mkdir $dir, 0770
or die "Roadkill: '$0' unable to make '$dir': $!";

my @filz = map { $dir . $_ } ( 'aa' .. 'at' );

eval { unlink @filz };

my @pass_1
= map
{
    my $path = $_;
    sub
    {
        open my $fh, '>', $path
        or die "Open: '$path', $!";

        0
    }
}
@filz;

my @pass_2
= map
{
    my $path = $_;
    sub
    {
        unlink $path
        or die "Unlink: $path: $!";

        0
    }
}
@filz;

for my $name ( qw( run_nofork ) )
{
    my $ref = qualify_to_ref $name, 'Parallel::Queue';

    undef &{ *$ref };

    *$ref   = sub { BAIL_OUT "Fork tests using run_nofork" };
}

########################################################################
# run in debug mode without forking/threading off
# the individual jobs.

my @buffer  = ();
my $n       = 0;

for my $i ( 1, 4 )
{
    @buffer = runqueue $i, @pass_1;
    $n      = @buffer;

    ok ! $n, "Pass 1 jobs remaining: $n (n=$i)";

    @buffer = grep { -e } @filz;

    ok @buffer ~~ @filz, "Files created (n=$i)";

    @buffer = runqueue $i, @pass_2;
    $n      = @buffer;

    ok ! $n, "Pass 2 jobs remaining: $n (n=$i)";

    @buffer = grep { -e } @filz;

    ok ! @buffer,  "Files removed (n=$i)";
}

eval { unlink glob "./tmp/$$*"; };
eval { rmdir "./tmp" };

done_testing;

########################################################################
# cleanup on the way out

eval
{
    unlink @filz;
};

__END__
