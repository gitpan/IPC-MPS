use 5.008008;
use ExtUtils::MakeMaker;


my @prereq_pm;
foreach (qw(Event EV)) {
	eval "require $_";
	if ($@) {
		my $prompt = prompt("Do you want to install $_?(Y/n)", 'Y');
		if ($prompt =~ m/[yY]/) {
			push @prereq_pm, $_ => 0;
		}
	}
}


WriteMakefile(
    NAME              => 'IPC::MPS',
    VERSION_FROM      => 'IPC/MPS.pm', # finds $VERSION
    PREREQ_PM         => { @prereq_pm }, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'IPC/MPS.pm', # retrieve abstract from module
       AUTHOR         => 'Nick Kostirya <nick@ToDo.org>') : ()),
);
