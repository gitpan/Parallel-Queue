########################################################################
# housekeeping
########################################################################

package Parallel::Queue;

use strict;

use Carp            qw( croak               );
use Scalar::Util    qw( looks_like_number   );
use Symbol          qw( qualify_to_ref      );

########################################################################
# package variables
########################################################################

our $VERSION    = '0.99';

# defaults.

our $debug      = '';
our $verbose    = '';
our $finish     = '';

########################################################################
# execution handlers
########################################################################

sub run_nofork
{
    # discard the count, iterate the queue.  the 
    # while loop allows returning the unused 
    # portion as when forked.

    shift;

    while( @_ )
    {
        my $sub     = shift;

        # these should all exit zero.

        my $result  = eval { $sub->() };

        if( $result )
        {
            print STDERR "Non-zero exit: $result, aborting queue";

            last
        }
        elsif( $@ )
        {
            print STDERR "Error in job: $@";

            last
        }
    }

    return @_
}

sub fork_proc
{
    if( ( my $pid = fork ) > 0 )
    {
        print STDERR "fork: $pid\n"
        if $verbose;

        # nothing useful to hand back.

        return
    }
    elsif( defined $pid )
    {
        # child passes the exit status of the perl sub call
        # to the caller as our exit status. the O/S will deal
        # with signal values.

        print STDERR "\tExecuting: $$\n"
        if $verbose;

        # failing to exit here will cause a runaway
        # if jobs are forked.

        exit $_[0]->()
    }
    else
    {
        # pass back the fork failure for the caller to deal with.

        die "Phorkafobia: $!";
    }
};

sub fork_queue
{
    # count was validated in runqueue.

    my $count = shift;

    # what's left on the stack are the jobs to run.
    # which may be none.
    # if so, we're done.

    print STDERR "Forking initial $count proc\n"
    if $verbose;

    # kick off count jobs to begin with, then 
    # start waiting.

    fork_proc $_ for splice @_, 0, $count;

    my @unused  = ();

    while( (my $pid = wait) > 0 )
    {
        print STDERR "exit: $pid ($?)\n"
        if $verbose;

        # this assumes normal *NIX 16-bit exit values,
        # with a status in the high byte and signum 
        # in the lower. notice that $status is not
        # masked to 8 bits, however. this allows us to
        # deal with non-zero exits on > 16-bit systems.
        #
        # caller can trap the signals.

        if( $? )
        {
            # bad news, boss...

            my $message
            = do
            {
                if( my $exit = $? >> 8 )
                {
                    "exit( $exit ) by $pid"
                }
                elsif( my $signal = $? & 0xFF )
                {
                    "kill SIG-$signal on $pid"
                }
            };

            $finish
            ? warn $message
            : die  $message
            ;
        }

        # kick off another job if the queue is not empty.

        my $sub = shift
        or next;

        fork_proc $sub;
    }

    return
};

# debug or zero count run the jobs without forking,
# simplifies most debugging issues.

sub runqueue
{
    my ( $count ) = @_;

    looks_like_number $count  
    or croak "Bogus runqueue: '$count' non-numeric";

    $count >= 0
    or croak "Bogus runqueue: '$count' must be non-negative";

    $debug || ! $count
    ? eval { &run_nofork }
    : eval { &fork_queue }
    ;

    # hand back the unused portion of the queue.

    @_
}

sub import
{
    # discard the current package name and deal 
    # with the args. empty arg for 'export' 
    # indicates that runqueue needs to be exported.

    my $caller = caller;

    shift;

    my $export  = 'runqueue';

    $debug      = !! $^P;
    $verbose    = '';
    $finish     = '';

    for( @_ )
    {
        my( $name, $arg ) = split /=/, $_;

        if(     'debug'     eq $name )
        {
            $debug      = defined $arg ? $arg : 1;
        }
        elsif(  'verbose'   eq $name )
        {
            $verbose    = defined $arg ? $arg : 1;
        }
        elsif(  'finish'    eq $name )
        {
            $finish     = defined $arg ? $arg : 1 ;
        }
        elsif(  'export'    eq $name )
        {
            $export     = $arg;
        }
        else
        {
            warn "Unknown argument: '$_' ignored";
        }
    }

    if( $export )
    {
        my $ref = qualify_to_ref $export, $caller;

        undef &{ *$ref };

        *$ref   = \&runqueue
    }

    return
}

*configure  = \&import;

# keep require happy

1

__END__

=head1 NAME

Parallel::Queue - fork list of subref's N-way parallel

=head1 SYNOPSIS

    # example queue:
    # only squish files larger than 8KB in size.  figure
    # that the system can handle four copies of squish
    # running at the same time without them interfering
    # with one another.

    my @queue = map { -s > 8192 ? sub{ squish $_ } : () } @filz;

    # simplest case: use the module and pass in 
    # the count and list of coderefs.

    use Parallel::Queue;

    my @remaining = runqueue 4, @queue;

    die "Incomplete jobs" if @remaining;

    # export allows changing the exported sub name.
    # "export=" allows not exporting it (which then
    # requires calling Parallel::Queue::runqueue ...

    use Parallel::Queue qw( export=handle_queue );

    my @remaining = handle_queue 4, @queue;

    # debug or a zero count (or running with the 
    # perl debugger) avoid forking. forking in
    # the debugger can be turned on with an 
    # explicit debug=0.

    #!/usr/bin/perl -d

    use Parallel::Queue qw( debug=0 );

    ...

    # or setting the debug variable to false.

    $Parallel::Queue::debug = '';

    runqueue ....

    # finish forces execution to continue even if 
    # there is an error in one job. this will finish
    # the cleanups even if one of them fails.

    use Parallel::Queue qw( finish );

    my @cleanupz    = ... ;

    runqueue $nway, @cleanupz;

    # "configure" is a more descriptive alias for the
    # import sub.

    Parallel::Queue->configure( debug=0 finish=1 );


=head1 DESCRIPTION

=head2 Arguments to use (or configure).

The finish, debug, and verbose arguments default
to true. This means that turning them on does 
not require an equals sign and value. Turning
them off requries an equal sign and zero.

The export option defaults to false: using it
without a value avoids exporting the runqueue
subroune into the caller.

=over 4

=item finish
=item finish=0

This causes the queue to finsih even if there are
non-zero exits on the way. Exits will be logged 
but will not stop the queue. 

=item export=my_name
=item export=

By default Parallel::Queue exports "runqueue", this can
be changed with the "export" arguments. In this case
call it "my_name" and use it to run the queue with two
parallel processes:

    use Parallel::Queue qw( export=run_these );

    my @queue       = ...;

    my @un_executed = run_these 2, @queue;

The name can be any valid Perl sub name.

Using an empty name avoids exporting anything 
and requires using the fully qualified subname
(Parallel::Query::runqueu) to run the queue.

=item verbose

This outputs a line with the process id each
time a job is dispatched or reaped.

=item debug

Turned on by default if the perl debugger is 
in use (via $^P), this avoids forking and 
simply dispatches the jobs one by one. This 
helps debug dispatched jobs where handling 
forks in the Perl debugger can be problematic.

Debug can be turned off via debug=0 with the
use or confgure. If this is turned off with
the debugger running then be prepared to supply
the tty's (see also Debugging forks below).

=back

=head1 KNOWN ISSUES

=over 4

=item Non-numeric count arguments.

The runqueue sub uses Scalar::Util::looks_like_number 
validate the count. This may cause problems for objects
which don't look like numbers.

=back

=head1 SEE ALSO

=item Debugging forks.

<http://qs321.pair.com/~monkads/index.pl?node_id=128283>

=head1 COPYRIGHT

This code is released under the same terms as Perl-5.8
or any later version of Perl.

=head1 AUTHOR

Steven Lembark <lembark@wrkhors.com>
