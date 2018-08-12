%define name            asdf
%define version         1

%define release			%(echo `uname -r|sed 's/-/_/'|cut -d . -f 1-4`)
%define install_dir 	/usr/local/bin
%define requires   		zlib curl
%define buildRequires   gcc
%define packager		xxx	
%define source			%{name}.tar.gz	
%define buildRoot		%{_tmppath}/%{name}-root	
%define summary			%{name} summary	
%define buildarch 		x86_64
%define debug_package 	%{nil}
%define group 			Applications/Internet
%define license			Commercial	
%define url				www.男生女生.cn
%define vendor			男生女生

Prefix:			/
Name:			%{name}
Version:		%{version}		
Release:		%{release}	
Summary:		%{summary}	
Group:			%{group}		
License:		%{license}
Source:			%{source}

AutoReq:   		0
Provides:		%{name}
Obsoletes:		%{name}
BuildRoot:		%{buildRoot}
BuildArch:		%{buildarch}	
URL:			%{url}		
Packager:		%{packager}
Vendor:			%{vendor}	
BuildRequires:	%{buildRequires} 	
Requires:		%{requires}	

%description

%prep
tar xf %{name}.tar.gz

%build
make 

%install
make install DEST_DIR=$RPM_BUILD_ROOT

%clean

%files
/usr/local/bin/*
/usr/local/lib/*
/etc/%{name}/*


%post
if [ $1 -eq 1 ]; then
	cron=`cat /var/spool/cron/root 2>/dev/null |grep '/etc/%{name}/%{name}-cron.sh' | grep -v "grep" | wc -l`
	if [ $cron -eq 0 ]; then
		echo "#*/1 * * * *  /etc/%{name}/%{name}-cron.sh " >> /var/spool/cron/root
	fi
fi
rm -rf /usr/lib64/libarchive.so.13
ln -s /usr/local/lib/libarchive.so.13 /usr/lib64/libarchive.so.13

ldconfig

%postun
if [ $1 -eq 0 ]; then
	sed -i '/%{name}-cron.sh/d' /var/spool/cron/root
fi
	
%changelog

