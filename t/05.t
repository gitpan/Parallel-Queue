########################################################################
# test job failure
########################################################################

use strict;

use Test::More qw( tests 4 );

use Parallel::Queue qw( verbose );

# depending on intra-job timing, there may be 
# one or two items left in @pass1 after the 
# queue is run once.

my @queue =
(
    sub {  0 },
    sub {  0 },

    sub {  1 },  # non-zero exit == failure.

    sub {  0 },  # these two are left on @pass1
    sub {  0 },
);


my @pass1   = runqueue 1, @queue;

my $count   = @pass1;

ok $count, "Two ($count) jobs remaining?";

ok $queue[-1] == $pass1[-1], 'Expected job unused';
ok $queue[-2] == $pass1[-2], 'Expected job unused';

my @pass2 = runqueue 8, @pass1;

ok ! @pass2, "Remaining jobs completed";

__END__
