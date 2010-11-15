use 5.008008;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME              => 'IPC::MPS',
    VERSION_FROM      => 'IPC/MPS.pm', # finds $VERSION
    PREREQ_PM         => {
		Event => 0,
		EV    => 0,
	}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'IPC/MPS.pm', # retrieve abstract from module
       AUTHOR         => 'Nick Kostirya <nick@ToDo.org>') : ()),
);
