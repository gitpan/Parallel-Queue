########################################################################
# housekeeping
########################################################################

package Parallel::Queue;

use strict;
use Carp;
use Symbol;
use Config;
use Scalar::Util qw( looks_like_number );

########################################################################
# package variables
########################################################################

our $VERSION = '0.03';

# default parallel behavior of forking is 
# handled via $arghash.

my %defaultz =
(
    fork    => 0,
    thread  => 0,

    debug   => 0,
    verbose => 0,
);

# per-package config values.

my %configz = ();

# populated via local value if threading is enabled.

our $semaphore  = '';

my %handlerz
= do
{
    ####################################################################
    # fork handlers
    #
    # $fork_proc has the parent clean off the call stack if the new pid
    # was created, othewise leave the stack alone and pass the failed
    # job back to the caller.
    #
    # $proc_loop 
    ####################################################################

    my $verbose     = 0;
    my $debug       = 0;

    my $semaphore   = '';

    my $setup 
    = sub
    {
        my $argz = shift;

        $verbose = $argz->{ verbose };
        $debug   = $argz->{ debug };

        # hand back the count, leaving the queue on 
        # the stack by itself.

        shift
    };

    my $fork_proc
    = sub
    {
        # the closure to run.

        if( (my $pid = fork()) > 0 )
        {
            # parent has to clean the item off of the stack

            print STDERR "fork: $pid\n"
            if $verbose;

            shift
        }
        elsif( defined $pid )
        {
            # child passes the exit status of the perl sub call
            # to the caller as our exit status. the O/S will deal
            # with signal values.
            #
            # the truly paranoid could return min( returncode, 255 ),
            # for now it's simpler to trust that most programs will
            # use small, non-zero (or negative) exits for errors.

            print STDERR "\tExecuting: $$\n"
            if $verbose;

            exit $_[0]->()
        }
        else
        {
            # pass back the fork failure for the caller to deal with.

            die "Phorkafobia: $!";
        }
    };

    my $fork_loop
    = sub
    {
        # block in wait for a process to exit
        # and deal with its exit status. if there
        # is nothing to wait for this falls through
        # and returns false.

        eval
        {
            my $count = &$setup;

            print STDERR "Forking initial $count proc\n"
            if $verbose;

            &$fork_proc for (1..$count);

            print STDERR "Looping remainder of list...\n"
            if $verbose;

            while( (my $pid = wait) > 0 )
            {
                print STDERR "exit: $pid ($?)\n"
                if $verbose;

                # this assumes normal *NIX 16-bit exit values,
                # with a status in the high byte and signum 
                # in the lower. notice that $status is not
                # masked to 8 bits, however. this allows us to
                # deal with non-zero exits on > 16-bit systems.

                if( $? )
                {
                    # bad news, boss...

                    if( my $status = $? >> 8 )
                    {
                        die "exit( $status ) by $pid";
                    }
                    elsif( my $signal = $? & 0xFF )
                    {
                        die "kill SIG-$signal on $pid";
                    }
                }

                # kick off another job if the queue is not empty.

                &$fork_proc if @_
            }
        };

        # caller gets back the undispatched portion of the
        # queue, which is false if all of the jobs completed.

        @_
    };

    ####################################################################
    # thread handlers.
    ####################################################################

    # split_thread blocks on the semaphore, so all the
    # thread loop has to do at this point is dispatch a
    # new copy of split_thread each time the splitter
    # returns. 
    #
    # at some point either @_ is exhausted or split_thread
    # dies.

    my $threadly 
    = sub
    {
        my $count = &$setup;

        my $semaphore = Thread::Semaphore->new( $count );

        while( @_ )
        {
            $semaphore->down;

            my $sub = $_[0];
            my $job
            = sub
            {
                eval { $sub->() };

                $semaphore->up;
            };

            if( my $thread = Threads->new($job) )
            {
                $thread->detach;

                shift;
            }
            else
            {
                die "Threadaphobia: $!";
            }
        }

        # wait for the last remaining jobs to finish.

        $semaphore->down( $count );
    };

    ####################################################################
    # map the mode (fork/thread) to the operations that perform it.
    ####################################################################

    (
        fork    => $fork_loop,

        thread  => $threadly,
    )
};

########################################################################
# import and construct both depend on $arghash to validate the config
# values, set the defaults, and remember previous values (via %configz).
#
# this is also where the runqueue sub is pushed out to the caller's
# space if requested.
#
# OO Interface allows creating separate queue managers with 
# pre-chosen thread/fork behavior and count of jobs to run.
#
# passing arguments to use allows setting per-package default
# values for the objects.

my $arghash 
= sub
{
    my $caller  = shift;

    my $defz    = $configz{ $caller } || \%defaultz;

    my %argz    = ();

    my %tmp     = ( %$defz, map { $_ => 1 } @_ );

    my $install = 'runqueue';

    # regex allows matching with or without a 
    # sigil on the subname.
    #
    # note that this can end up installing 
    # runqueue for OO modules that call the
    # constructor with a 'runqueue' argument.

    if( my ($subkey) = grep /$install/o, keys %tmp )
    {
        delete $tmp{ $subkey };

        my $ref = qualify_to_ref $install, $caller; 

        *$ref = __PACKAGE__->can( $install );
    }

    $tmp{ debug } ||= 1 if $^P;

    # default is to fork.

    $tmp{ fork } ||= ! $tmp{ thread };

    @argz{ keys %defaultz } = delete @tmp{ keys %defaultz };

    croak 'Bogus extra arguments: ', %tmp
    if %tmp;

    croak 'Bogus Parallel::Queue: "fork" and "thread" are exclusive'
    if $argz{ fork } && $argz{ thread };

    if( $argz{ thread } )
    {
        # sanity check version, ithreads, and 
        # sempaphore module only if threads are
        # used.

        croak 'Bogus Parallel::Queue: ithreads not available'
        unless $Config{useithreads};

        require 5.8.0;

        require Thread::Semaphore;
    }

    # at this point the arguments seem usable.

    \%argz
};

sub import
{
    # discard the current package name and deal 
    # with the args.

    my $caller = caller;

    splice @_, 0, 1, $caller;

    # what's left are a list of argument => value pairs
    # that are hashable.
    #
    # debug mode is automatically turned on in the debugger.
    # if it isn't already. this might be better handled via
    # defined, but most people don't want to modify their
    # code to have a 'debug => 0' in order to debug it.

    $configz{ $caller } = &$arghash;
}

sub construct
{
    # the first argument means something here: it's
    # the class prototype (via name or ref). 

    my $caller = caller;

    my $proto = splice @_, 0, 1, $caller;

    my $argz = &$arghash;

    bless sub { $argz }, ref $proto || $proto
}

####################################################################
# top half of the execution engine. this validates the arguments,
# locates the fork/thread handler, and dispatches it.
#
# the work to determine $argz comes from this being called as
# both a function (count first) or object (class/object first).
#
# after the config arguments have been extracted and the count
# is first on the stack, execution is identical for functinal 
# and OO execution.

sub runqueue
{ 
    # two ways in here: calling a class method or 
    # using an installed runqueue.

    my $argz
    = do
    {
        # extract the arguments from the object or
        # from the calling packages's config value.
        #
        # note that the first argument might be an 
        # object, but if it isn't an instance of 
        # this package then it probably isn't useful
        # for extracting the config data.

        my $caller = caller;

        my $pkg = __PACKAGE__;

        if( looks_like_number $_[0] )
        {
            # leading number gets its config from the 
            # use arguments.

            $configz{ $caller }
            or die "Bogus runqueue: import bypassed, no config for '$caller'";
        } 
        else
        {
            # if it isn't a number it'd better be derived
            # from this package...

            my $item = shift;

            $item->isa( $pkg )
            or croak "Bogus runqueue: '$item' is not a '$pkg'";

            ref $item
            ? $item->()
            : $arghash->('')
        }
    };

    my $debug = $argz->{ debug };

    $DB::single = 1 if $debug;

    # the stack is now a job count followed by the
    # coderef's to dispatch.

    my $count = $_[0];

    looks_like_number $count
    or croak "Bogus job count: '$_[0]' does not look like a number";
    
    $count >= 0
    or croak "Bogus runqueue: '$count' < 0";

    $DB::single = 1 if $debug;

    if( $count )
    {
        my $mode = $argz->{ fork } ? 'fork' : 'thread';

        if( my $handler = $handlerz{ $mode } )
        {
            # prefix the stack with the arguments and 
            # descend into hell... er, the bottom half.

            splice @_, 0, 0, $argz;

            goto &$handler;

            die "Roadkill: Failed dispatching '$handler' for '$mode': $!"
        }
        else
        {
            croak "Bogus mode: no dispatcher for '$mode'";
        }
    }
    else
    {
        # count of zero doesn't split off anything, mainly
        # for debugging the code single-stream.

        shift;

        while( my $sub = shift @_ )
        {
            last if $sub->()
        }

        @_
    }

    # normal execution never gets this far since it enters
    # the handler via goto; single stream shouldn't need any
    # followup.
}

# keep require happy

1

__END__

=head1 NAME

Parallel::Queue - fork or thread a list of closures N-way parallel

=head1 SYNOPSIS

    # example queue:
    # only squish files larger than 8KB in size.  figure
    # that the system can handle four copies of squish
    # running at the same time without them interfering
    # with one another.

    my @queue = map { -s > 8192 ? sub{ squish $_ } : () } @filz;

    # functional: pass in the count and list of coderefs.
    #
    # adding 'runqueue' exports the subroutine into
    # the current package. useful for non-OO situations.
    #
    # run the queue 4 way parallel.

    use Parallel::Queue qw( runqueue verbose fork );

    runqueue 4, @queue;

    die "Incomplete jobs" if @queue;

    # OO: generate queue manager and use without the 
    # 'runqueue' arguments, construct a queue manager,
    # and use it to run the jobs

    use Parallel::Queue;

    my $quemgr = Parallel::Queue->construct( thread );

    $quemgr->runqueue( 4, @queue );

    die "Incomplete jobs" if @queue;

    # call Parallel::Queue with the default configuration
    # (fork quietly).

    require Parallel::Queue;

    Parallel::Queue->runqueue( 4, @queue );

    # pre-define defaults for the objects: leave
    # out runqueue, set the rest, and construct 
    # an object. the one here gets verbose, thread,
    # and debug all set to true.

    use Parallel::Queue qw( verbose thread );

    my $quemgr = Parallel::Queue->construct( debug );

    $quemgr->runqueue( 4, @queue );



=head1 DESCRIPTION

Given a count and an array of coderefs (most likely closures),
runqueue will run the jobs in parallel. The jobs can
be run via fork or detached threads [see known issues
for threading].  Jobs on the queue are executed until
one of them exits non-zero, the fork/thread operation
fails, or all of them are dispatched (i.e., the queue
is empty).

=head2 Functional

Parallel::Queue does not export the runqueue function by
default. Adding 'runqueue' to the argument list exports
the subroutine into the caller's space and additinally 
stores the remaining arguments indexed by the caller's
package (i.e., multiple modules can use Parallel::Queue
and get different behavior).

    # get the subroutine installed locally, run the
    # queue 8-ways parallel.

    use Parallel::Queue qw( runqueue verbose );

    runqueue 8, @queue;

=head2 Objective

If runqueue is not exported it can be accessed as an 
object or class method. Objects are constructed with
the same arguments as use, after which runqueue is 
called via the object. This allows pushing things
like fork vs. thread decisions into runtime code:

    use Parallel::Queue;

    ...

    my $how = $threadly ? 'thread' : 'fork';

    ...

    my $que_mgr = Parallel::Queue->construct( $how, $verbose );

    $que_mgr->runqueue( 8, @queue );

If runqueue is called as a class method then it will
run with the default configuration: forking quietly.

    require Parallel::Queue;

    Parallel::Queue->runqueue( 8, @queue );


this is the equivalent of using:

    use Parallel::Queue qw( runqueue );

    runqueue  8, @queue;

but may be easier in cases where the module is being included
at runtime (usually via require).

=head3 Constructor defaults via use.

Arguments passed to use are installed as defaults for
the calling class. This allows a class to determine 
defaults for the objects created, which can be useful
for reducing the amount of data thrown around in the 
construction.

    # default has verbose turned on.

    use Parallel::Queue qw( verbose );

    ...

    # gets verbose and fork turned on.

    my $quemgr = Parallel::Queue->construct( fork ); 


=head1 KNOWN ISSUES

=over 4

=item Non-numeric count arguments.

The runqueue sub uses Scalar::Util::looks_like_number to
determine if it is being called as a function or method.
Basically, if the first argument looks like a number then 
it isn't being used via an OO call.

If a class that doesn't look like a number with numeric
overloading is used for the count then this could cause
problems. This will show up as runqueue complaining that
the first argument is not a Parallel::Queue (i.e., that
$_[0]->isa( __PACKAGE__ ) is false).

=item Threads are unstested.

Forks work; threads are still in progress. The code 
may work but I have not tested it yet.

=back

=head1 SEE ALSO

=item Debugging forks.

<http://qs321.pair.com/~monkads/index.pl?node_id=128283>

=head1 COPYRIGHT

This code is released under the same terms as Perl-5.8
or any later version of Perl.

=head1 AUTHOR

Steven Lembark <lembark@wrkhors.com>
