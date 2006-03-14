########################################################################
# first sanity check: is the module usable.
########################################################################

use strict;
use FindBin::libs;

use Test::More qw( tests 2 );

use_ok( 'Parallel::Queue' );

my $version = Parallel::Queue->VERSION;

ok( $version, 'Parallel::Queue::VERSION' );

__END__
