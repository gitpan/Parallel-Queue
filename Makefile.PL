use 5.006;
use ExtUtils::MakeMaker;

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.


WriteMakefile
(
    NAME            => 'Parallel::Queue',
    AUTHOR          => 'Steven Lembark <lembark@wrkhors.com>',

    VERSION         => '2.00',
    ABSTRACT_FROM   => 'lib/Parallel/Queue.pm',

	PREREQ_PM =>
	{
        # take whatever versions are in the standard distro.

		strict      => 0,
		Carp        => 0,
        Symbol      => 0,
        Config      => 0,
	},

);

