Name:		sassrv
Version:	999.999
Vendor:	        Rai Technology, Inc
Release:	99999%{?dist}
Summary:	Rai Sass RV

License:	ASL 2.0
URL:		https://github.com/raitechnology/%{name}
Source0:	%{name}-%{version}-99999.tar.gz
BuildRoot:	${_tmppath}
Prefix:	        /usr
BuildRequires:  gcc-c++
BuildRequires:  chrpath
BuildRequires:  raikv
BuildRequires:  raimd
BuildRequires:  libdecnumber
BuildRequires:  pcre2-devel
BuildRequires:  hdrhist
BuildRequires:  git-core
Requires:       raikv
Requires:       raimd
Requires:       libdecnumber
Requires:       pcre2
Requires:       hdrhist
Requires(post): /sbin/ldconfig
Requires(postun): /sbin/ldconfig

%description
Rai Sass RV protocol

%prep
%setup -q


%define _unpackaged_files_terminate_build 0
%define _missing_doc_files_terminate_build 0
%define _missing_build_ids_terminate_build 0
%define _include_gdb_index 1

%build
make build_dir=./usr %{?_smp_mflags} dist_bins
cp -a ./include ./usr/include

%install
rm -rf %{buildroot}
mkdir -p  %{buildroot}

# in builddir
cp -a * %{buildroot}

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%{_bindir}/*
%{_prefix}/lib64/*
%{_includedir}/*

%post
echo "%{_prefix}/lib64" > /etc/ld.so.conf.d/%{name}.conf
/sbin/ldconfig

%postun
if [ $1 -eq 0 ] ; then
rm -f /etc/ld.so.conf.d/%{name}.conf
fi
/sbin/ldconfig

%changelog
* __DATE__ <support@raitechnology.com>
- Hello world
