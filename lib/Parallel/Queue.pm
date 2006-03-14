
package Parallel::Queue;

use strict;
use Carp;
use Symbol;
use Config;
use Scalar::Util qw( looks_like_number );

our $VERSION = '0.01';

# default parallel behavior is fork, see 
# hashargs.

my %defaultz =
(

    fork    => 0,
    thread  => 0,

    debug   => 0,
    verbose => 0,
);

# per-package config values.

my %config = ();

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

    my $verbose = 0;
    my $debug   = 0;

    my $setup 
    = sub
    {
        my $argz = shift;

        $verbose = $argz->{ verbose };
        $debug   = $argz->{ debug };
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

    my $proc_loop
    = sub
    {
        # block in wait for a process to exit
        # and deal with its exit status. if there
        # is nothing to wait for this falls through
        # and returns false.

        eval
        {
            print STDERR "Waiting..."
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

    my $split_thread 
    = sub
    {
        # block here to avoid creating the thread
        # until it is runnable. this leaves the 
        # stack intact until the job can be started
        # in case the queue manager aborts.

        $semaphore->down;

        my $run = $_[0]
        or die "Bogus threadify: missing job to run";

        # since the threads are detached they will have
        # to up the semaphore for themselves.

        my $job = 
        sub
        {
            $run->();

            $semaphore->up;
        };

        if( my $thread = Threads->new($job) )
        {
            $thread->detach;
        }
        else
        {
            die "Threadaphobia: $!";
        }

        # now that the job has been started, drop it
        # off of the queue.

        shift;
    };

    # split_thread blocks on the semaphore, so all the
    # thread loop has to do at this point is dispatch a
    # new copy of split_thread each time the splitter
    # returns. 
    #
    # at some point either @_ is exhausted or split_thread
    # dies.

    my $thread_loop
    = sub
    {
        eval { &$split_thread while @_ };

        @_
    };

    ####################################################################
    # map the mode (fork/thread) to the operations that perform it.
    ####################################################################

    (
        fork =>
        {
            init    => $setup,
            job     => $fork_proc,
            loop    => $proc_loop,
        },

        thread =>
        {
            init    => $setup,
            job     => $split_thread,
            loop    => $thread_loop,
        },
    )
};

########################################################################
# push the runqueue sub into the caller's
# namespace.
#
# discard this pacakge, grab the arg's from 
# whatever's left.
#
# args are stored per packge: multiple "use"
# statements can set separate arguments.
#
# notice that the OO and functional interfaces
# use differing versions of the code available
# as "runqueue" to the caller.

my $arghash 
= sub
{
    my $caller  = shift;

    my %argz    = ();

    my %tmp     = ( %defaultz, map { $_ => 1 } @_ );

    my $install = 'runqueue';

    # regex allows matching with or without a 
    # sigil on the subname.
    #
    # checking for a valid caller allows short-
    # circuting this for OO calls.

    if( my ($subkey) = grep /$install/o, keys %tmp )
    {
        delete $tmp{ $subkey };

        if( $caller )
        {
            my $ref = qualify_to_ref $install, $caller; 

            *$ref = __PACKAGE__->can( $install );
        }
    }

    $tmp{ debug } ||= 1 if $^P;

    # default is to fork.

    $tmp{ fork } = 1 unless $tmp{ thread };

    @argz{ keys %defaultz } = delete @tmp{ keys %defaultz };

    croak 'Bogus extra arguments: ', %tmp
    if %tmp;

    croak 'Bogus Parallel::Queue: "fork" and "thread" are exclusive'
    if $argz{ fork } && $argz{ thread };

    if( $argz{ thread } )
    {
        croak 'Bogus Parallel::Queue: ithreads not available'
        unless $Config{useithreads};

        require Thread::Semaphore;
    }

    \%argz
};

sub import
{
    # discard the current package name and deal 
    # with the args. passing in unknown arguments
    # is fatal.
    #
    # if the next argument is "runqueue" (with
    # or without a sigil) then install it.

    my $caller = caller;

    splice @_, 0, 1, $caller;

    # what's left are a list of argument => value pairs
    # that are hashable.
    #
    # debug mode is automatically turned on in the debugger.
    # if it isn't already. this might be better handled via
    # defined, but most people don't want to modify their
    # code to have a 'debug => 0' in order to debug it.

    $config{ $caller } = &$arghash;
}

####################################################################
# aside from the process of extracting arguments for the queue,
# functional and OO code are identical: pass the arg's and job
# count here along with the queue to run.
#
# OO Interface allows creating separate queue managers with 
# pre-chosen thread/fork behavior and count of jobs to run.

sub construct
{
    my $proto = splice @_, 0, 1, '';

    my $argz = &$arghash;

    bless sub { $argz }, ref $proto || $proto
}

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

            $config{ $caller }
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
            : { %defaultz }
        }
    };

    # what is left on the stack at this point are the 
    # job count and jobs to run.

    my $count = shift;
    
    $count >= 0
    or croak "Bogus runqueue: count must be >= 0";

    my $debug   = $argz->{ debug };

    if( $count == 0 )
    {
        # run the jobs single-stream, mainly to avoid 
        # issues debugging fork/thread.
        #
        # setting a breakpoint at the $_->() line 

        for( @_ )
        {
            $DB::single = 1
            if $debug;

            $_->()
        }
    }
    elsif( $count > 0 )
    {
        # whatever's left on the arg list is the queue...
        # now to massage the arg's for handling the current
        # queue's execution.

        $DB::single = 1 if $argz->{ debug };

        local $semaphore = Thread::Semaphore->new( $count )
        if $argz->{ thread };

        my $mode = $argz->{ fork } ? 'fork' : 'thread';

        my ( $setup, $dispatch, $waitloop )
        = @{ $handlerz{ $mode } }{ qw( init job loop ) }
        or croak "Bogus mode: no dispatcher for '$argz->{mode}'";

        # start the queue by dispatching the first count items
        # then call the loop to finish the argument list.

        eval
        {
            $setup->( $argz );

            &$dispatch for 1..$count;

            # caller gets back whatever portion of the queue was 
            # passed back by the looping operator.

            &$waitloop
        };
    }
    else
    {
        croak "Bogus runqueue: invalid job count: '$count' should be >= 0";
    }

    # caller gets back false if all of the jobs
    # were dispatched.

    $DB::single = 1 if $debug;

    @_
}


# keep require happy

1

__END__

=head1 NAME

Parallel::Queue - fork or thread a list of closures N-way parallel

=head1 SYNOPSIS

    # use the OO interface with separate configuration
    # data for each queue manager (notice the lack of
    # 'runqueue').

    use Parallel::Queue qw( verbose debug );

    # only squish files larger than 8KB in size.
    # figure that the system can handle four copies
    # of squish running at the same time without 
    # them interfering with one another.

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

    Parallel::Queue->runqueue( $count, @queue );


=head1 DESCRIPTION

Given a count and an array of coderefs (most likely closures), 
runqueue will run the jobs in parallel. The jobs can be run 
via fork or (detached) threads. Jobs on the queue are executed
until one of them exits non-zero, the fork/thread operation fails,
or all of them are dispatched (i.e., the queue is empty).

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

=back

=head1 SEE ALSO

=item Debugging forks.

<http://qs321.pair.com/~monkads/index.pl?node_id=128283>

=head1 COPYRIGHT

This code is released under the same terms as Perl-5.8
or any later version of Perl.

=head1 AUTHOR

Steven Lembark <lembark@wrkhors.com>
