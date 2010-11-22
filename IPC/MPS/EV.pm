package IPC::MPS::EV;

use strict;
use warnings;

use Exporter;
our @ISA = qw(Exporter);
our @EXPORT = qw(spawn receive msg snd wt snd_wt listener open_node);

our $VERSION = '0.09';

use Carp;
use EV;
use IO::Socket;
use Scalar::Util qw(refaddr);
use Storable qw(freeze thaw);


my $DEBUG = 0;
$DEBUG and require Data::Dumper;

my $loop = EV::Loop->new();
my @spawn            = ();
my %msg              = ();
my %fh2vpid          = ();
my %vpid2fh          = ();
my %fh2fh            = ();
my $self_vpid        = 0;
my $self_parent_fh;
my $self_parent_vpid = 0;
my %listener         = ();
my %node             = ();
my %snd              = ();
my $ipc_loop         = 0; 

my @rcv    = ();
my %r_bufs = ();
my %w_bufs = ();

my %fh2rw = ();
my %fh2ww = ();

my ($waited_vpid, $waited_msg, @waited_rv);

my $blksize = 1024 * 16;

END {
	$ipc_loop or @spawn and carp "Probably have forgotten to call receive.";
	close $_ foreach values %fh2fh;
}

sub spawn(&) {
	my ($spawn) = @_;
	socketpair(my $child, my $parent, AF_UNIX, SOCK_STREAM, PF_UNSPEC) or die "socketpair: $!";
	my $vpid = refaddr $child;
	push @spawn, [$vpid, $child, $parent, $spawn];
	return $vpid;
}


sub msg($$) {
	my ($msg, $sub) = @_;
	$msg{$msg} = $sub;
}


sub snd($$;@) {
	my ($vpid, $msg, @args) = @_;
	defined $vpid or carp("Argument vpid required"), return;
	defined $msg  or carp("Argument msg required"),  return;
	$vpid = $self_parent_vpid if $vpid == 0;
	$DEBUG and print "Send message '$msg' from $self_vpid to $vpid vpid in $self_vpid (\$\$=$$) with args: ", join(", ", @args), ".\n";
	push @{$snd{$vpid}}, [$self_vpid, $vpid, $msg, \@args];
	w_event_cb_reg($vpid);
}


sub snd_wt($$;@) {
	my ($vpid, $msg, @args) = @_;
	defined $vpid or carp("Argument vpid required"), return;
	defined $msg  or carp("Argument msg required"),  return;
	snd($vpid, $msg, @args);
	wt($vpid, $msg);
}


sub listener($$) {
	my ($host, $port) = @_;
	defined $host or carp("Argument host required"), return;
	defined $port or carp("Argument port required"), return;
	my $sock = IO::Socket::INET->new(Proto => 'tcp', Blocking => 0, LocalHost => $host, LocalPort => $port, Listen => 20, ReuseAddr => 1);
	if ($sock) {
		$listener{$sock} = $sock;
		$fh2rw{$sock} = $loop->io($sock, EV::READ, sub {
			my $w = shift;
			my $fh = $w->fh;
			$DEBUG > 1 and print "Read event for listener from $self_vpid: \n";
			my $sock = $fh->accept;
			$sock->sockopt(SO_KEEPALIVE, 1);
			my $vpid = refaddr $sock;
			$node{$sock}     = $vpid;
			$fh2vpid{$sock}  = $vpid;
			$vpid2fh{$vpid}  = $sock;
			$fh2fh{$sock}    = $sock;
			$fh2rw{$sock} = $loop->io($sock, EV::READ, \&r_event_cb);
		});
		return $sock;
	} else {
		carp "Cannot open socket '$host:$port' in $self_vpid: $!";
		return;
	}
}


sub open_node($$) {
	my ($host, $port) = @_;
	defined $host or carp("Argument host required"), return;
	defined $port or carp("Argument port required"), return;
	my $sock = IO::Socket::INET->new(Proto => 'tcp', Blocking => 0);
    my $addr = sockaddr_in($port,inet_aton($host));
	$sock->sockopt(SO_KEEPALIVE, 1);
    my $rv = $sock->connect($addr);
	if ($rv) {
		my $vpid = refaddr $sock;
		$node{$sock}     = $vpid;
		$fh2vpid{$sock}  = $vpid;
		$vpid2fh{$vpid}  = $sock;
		$fh2fh{$sock}    = $sock;
		$fh2rw{$sock} = $loop->io($sock, EV::READ, \&r_event_cb);
		return $vpid;
	} else {
		carp "Cannot connect to socket '$host:$port' in $self_vpid: $!";
		return;
	}
}



sub receive(&) {
	my ($receive) = @_;

	$DEBUG > 1 and print "Call receive in $self_vpid (\$\$=$$)\n";

	local $SIG{CHLD} = "IGNORE";
	local $SIG{PIPE} = "IGNORE";

	foreach (@spawn) {
		my ($vpid, $child, $parent, $spawn) = @$_;
		
		my $kid_pid = fork;
		defined $kid_pid or die "Can't fork: $!";

		unless ($kid_pid) {
			
			foreach (@spawn) {
				close $$_[1];
				close $$_[2] if $$_[2] ne $parent;
			}

			close $_ foreach values %fh2fh, values %listener;
			$loop = EV::Loop->new();
			@spawn    = ();
			%listener = ();
			%node     = ();
			%msg      = ();
			%fh2vpid  = ();
			%vpid2fh  = ();
			%fh2fh    = ();
			%snd      = ();

			$ipc_loop = 0;

			@rcv    = ();
			%r_bufs = ();
			%w_bufs = ();

			%fh2rw = ();
			%fh2ww = (); 

			($waited_vpid, $waited_msg, @waited_rv) = ();

			$self_parent_fh   = $parent;
			$self_parent_vpid = $self_vpid;

			$self_vpid        = $vpid;

			$fh2vpid{$self_parent_fh}   = $self_parent_vpid;
			$vpid2fh{$self_parent_vpid} = $self_parent_fh;
			$fh2fh{$self_parent_fh}     = $self_parent_fh;

			$fh2rw{$self_parent_fh} = $loop->io($self_parent_fh, EV::READ, \&r_event_cb);

			$spawn->();

			exit;
		}
	}


	foreach (@spawn) {
		my ($vpid, $child, $parent, $spawn, $receive) = @$_;
		close $parent;
		$fh2vpid{$child} = $vpid;
		$vpid2fh{$vpid}  = $child;
		$fh2fh{$child}   = $child;
		$fh2rw{$child} = $loop->io($child, EV::READ, \&r_event_cb);
	}
	@spawn = ();



	$receive->();



	unless ($ipc_loop) {
		$ipc_loop = 1;
		w_event_cb_reg();
		$loop->loop();
	}
}


sub wt($$) {
	($waited_vpid, $waited_msg) = @_;
	defined $waited_vpid or carp("Argument vpid required"), return;
	defined $waited_msg  or carp("Argument msg required"),  return;
	$waited_vpid = $self_parent_vpid if $waited_vpid == 0;
	foreach my $i (0 .. $#rcv) {
		my ($from, $msg, $args)= @{$rcv[$i]};
		if ($from eq $waited_vpid and $msg eq $waited_msg) {
			splice @rcv, $i, 1;
			return wantarray ? @$args : $$args[0];
		}
	}
	$DEBUG and print "Start waiting for '$waited_vpid -> $waited_msg' in $self_vpid (\$\$=$$)\n";
	w_event_cb_reg();
	$loop->loop();
	my @rv = @waited_rv;
	($waited_vpid, $waited_msg, @waited_rv) = ();
	return wantarray ? @rv : $rv[0];
}


sub w_event_cb_reg {
	my ($to_vpid) = @_;

		foreach my $to (defined $to_vpid ? $to_vpid : keys %snd) {
			if (@{$snd{$to}}) {
				my $fh = $vpid2fh{$to};
				unless ($fh) {
					if (@spawn) {
						carp "Probably have forgotten to call receive." if not defined $to_vpid;
						next;
					} else {
						if ($self_parent_fh) {
							$fh = $self_parent_fh;
						} else {
							carp "The addressee $to is unknown or has left in $self_vpid (\$\$=$$)\n";
							next;
						}
					}
				}
				unless (exists $w_bufs{$fh}) {
					my $packet = freeze shift @{$snd{$to}};
					my $buf = join "", pack("N", length $packet), $packet;
					$w_bufs{$fh} = $buf;
					$DEBUG and (@{$snd{$to}} or delete $snd{$to});
					$fh2ww{$fh} = $loop->io($fh, EV::WRITE, \&w_event_cb);
				}
			}
		}
}




sub r_event_cb {
	my $w = shift;
	my $fh = $w->fh;

	$DEBUG > 1 and print "Read event from $self_vpid: \n";

			my $len = sysread $fh, (my $_buf), $blksize;
 			if ($len) {
				$r_bufs{$fh} .= $_buf;
				NEXT_MSG: {
					my $buf = $r_bufs{$fh};
					if (length $buf >= 4) {
						my $packet_length = unpack "N", substr $buf, 0, 4, "";
						if (length $buf >= $packet_length) {
							my $packet = substr $buf, 0, $packet_length, "";
							$r_bufs{$fh} = $buf || "";
							$DEBUG and ($r_bufs{$fh} or delete $r_bufs{$fh});

							my ($from, $to, $msg, $args) = @{thaw $packet};

							if ($node{$fh}) {
								$from = $node{$fh};
								$to   = $self_vpid;
							}

							$DEBUG and print "Got message '$msg' from $from to $to vpid in $self_vpid (\$\$=$$) with args: ", join(", ", @$args), ".\n";
							if ($to == $self_vpid) {
								$DEBUG and print "Run message sub '$msg' from $from to $to vpid in $self_vpid (\$\$=$$) with args: ", join(", ", @$args), ".\n";
								if (defined $waited_vpid and defined $waited_msg) {
									push @rcv, [$from, $msg, $args];
								} else {
									if ($msg{$msg}) {
										push @rcv, [$from, $msg, $args];
									} else {
										$DEBUG and print "Unknown message '$msg'\n";
									}
								}
							} elsif ($vpid2fh{$to}) {
								$DEBUG and print "Remittance message '$msg' from $from to $to vpid in $self_vpid (\$\$=$$) with args: ", join(", ", @$args), ".\n";
								push @{$snd{$to}}, [$from, $to, $msg, $args];
								w_event_cb_reg();
							} else {
								carp "Got Wandered message '$msg' from $from to $to in $self_vpid (\$\$=$$)\n";
							}

							redo NEXT_MSG if $r_bufs{$fh};
						}
					}
				}
 			} elsif (defined $len) {
				if (exists $fh2ww{$fh}) {
					delete $fh2ww{$fh};
				}
				delete $fh2rw{$fh};
 				delete $r_bufs{$fh};
 				delete $w_bufs{$fh};
				delete $fh2fh{$fh};
				delete $vpid2fh{$fh2vpid{$fh}};
				delete $fh2vpid{$fh};
				if (my $vpid = $node{$fh}) {
					delete $node{$fh};
					if ($msg{NODE_CLOSED}) {
						$msg{NODE_CLOSED}->($vpid);
						w_event_cb_reg();
					}
				}
 				close $fh;
				if ($self_parent_fh and $self_parent_fh eq $fh) {
					unless (defined $waited_vpid and defined $waited_msg) {
						unless (@rcv) {
							exit;
						}
					}
				}
 			} else {
 				$DEBUG and die "Can't read '$fh': $!";
 			}

		if (defined $waited_vpid and defined $waited_msg) {
			foreach my $i (0 .. $#rcv) {
				my ($from, $msg, $args)= @{$rcv[$i]};
				if ($msg eq $waited_msg and $from eq $waited_vpid) {
					splice @rcv, $i, 1;
					$DEBUG and print "Stop waiting for '$waited_vpid -> $waited_msg' in $self_vpid (\$\$=$$)\n";
					@waited_rv = @$args;
					$loop->unloop();
					return;
				}
			}
			unless (exists $vpid2fh{$waited_vpid}) {
				$loop->unloop();
				return;
			}			
		} else {
			while (my $rcv = shift @rcv) {
				my ($from, $msg, $args)= @{$rcv};
				$msg{$msg}->($from, @$args);
				w_event_cb_reg();
			}
		}
}



sub w_event_cb {
	my $w = shift;
	my $fh = $w->fh;

	$DEBUG > 1 and print "Write event from $self_vpid: \n";
	$fh2fh{$fh} or return;

			my $buf = $w_bufs{$fh};
			my $len = syswrite $fh, $buf, $blksize;
			if ($len) {
				substr $buf, 0, $len, "";
				if (length $buf) {
					$w_bufs{$fh} = $buf;
				} else {
					delete $w_bufs{$fh};
					delete $fh2ww{$fh};
					w_event_cb_reg();
				}
			} else {
 				$DEBUG and die "Can't write to '$fh': $!";
			}
}



1;


__END__


=head1 NAME

IPC::MPS::EV - IPC::MPS based on L<EV>

=head1 DESCRIPTION

See description in L<IPC::MPS>.

=head1 AUTHOR

Nick Kostirya

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2009 by Nick Kostirya

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut
