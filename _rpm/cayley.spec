%define   debug_package %{nil}
Name:     cayley
Version:  0.4.1
Release:  1%{?dist}
Summary:  Cayley is an open-source graph database written in go.

Group:          Applications/Databases
License:        ASL 2.0
URL:            https://github.com/google/cayley
Source0:        https://github.com/google/%{name}/archive/v%{version}.tar.gz#/%{name}-%{version}.tar.gz
BuildRequires:  epel-release, golang < 2.0
Requires(pre): /usr/sbin/useradd, /usr/bin/getent

%description

Cayley is an open-source graph inspired by the graph database behind Freebase
and Google's Knowledge Graph.

Its goal is to be a part of the developer's toolbox where Linked Data and
graph-shaped data (semantic webs, social networks, etc) in general
are concerned.

%prep
%autosetup -n %{name}-%{version}

%build
%__mkdir_p _build/src/github.com/google/
pushd _build
%__ln_s $(dirs +1 -l) src/github.com/google/cayley
# This is a bit dirty, because we are not defining it in BuildRequires
# However, this would require an rpm for godep
GOPATH="$(pwd)" go get github.com/tools/godep
popd
GOPATH="$(pwd)/_build" _build/bin/godep restore
GOPATH="$(pwd)/_build" go build ./cmd/cayley

%install
%__install -d  %{buildroot}%{_unitdir}
%__install -m 0644 _rpm/cayley.service %{buildroot}%{_unitdir}
%__install -d  %{buildroot}%{_bindir}
%__install -m 0755 cayley %{buildroot}%{_bindir}
%__install -d %{buildroot}%{_datarootdir}/cayley/assets/
%__install -d %{buildroot}%{_localstatedir}/lib/cayley/
%__install -d %{buildroot}%{_sysconfdir}/systemd/system/cayley.service.d/
%__install -m 0600 _rpm/cayley.env.conf %{buildroot}%{_sysconfdir}/systemd/system/cayley.service.d/custom.env.conf
%__cp -a docs/ templates/ static/ %{buildroot}%{_datarootdir}/cayley/assets/

%pre
/usr/bin/getent passwd cayley || /usr/sbin/useradd -d /var/lib/cayley -r -s /sbin/nologin cayley

%post
if [ ! -f /var/lib/cayley/cayley.db ]; then
sudo -u cayley /usr/bin/cayley init -dbpath="/var/lib/cayley/cayley.db" -db="bolt"
fi

%preun
systemctl stop cayley

%files
%doc AUTHORS CONTRIBUTING.md LICENSE README.md CONTRIBUTORS TODO.md
%defattr(0644, cayley, cayley, 0755)
%attr(0755,root,root) /usr/bin/cayley
/usr/share/*
%{_unitdir}/cayley.service
%config(noreplace) %{_sysconfdir}/systemd/system/cayley.service.d/custom.env.conf
%dir %_localstatedir/lib/cayley/

%changelog
* Thu Feb 18 2016 Markus Mahlberg <markus.mahlberg@icloud.com> - 0.4.1-1
- Initial RPM release
