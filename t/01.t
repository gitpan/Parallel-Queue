########################################################################
# first sanity check: is the module usable.
########################################################################

use strict;

my $package = 'Parallel::Queue';

use Test::More qw( tests 2 );

use_ok $package;

my $version = $package->VERSION;

ok $version , "$package version $version";

__END__
