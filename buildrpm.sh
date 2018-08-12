#!/bin/bash
if [ ! -n "$1" ]; then
	echo "./rpmbuild.sh [\$1 is null] ,check Makefile"
	echo "sh name($1) version($2)"
	exit
fi
if [ ! -n "$2" ]; then
	echo "./rpmbuild.sh [\$2 is null] ,check Makefile"
	echo "sh name($1) version($2)"
	exit
fi

name=$1
version=$2
BUILDARCH=`uname -m`

spec_tf=.createrpm_temp.spec

rm -f $spec_tf
echo  "%define name            $name" >> $spec_tf
echo  "%define version         $version" >> $spec_tf
cat spec >> $spec_tf
tar zcf $name.tar.gz ./
rm -rf ~/rpmbuild/*
mkdir -p ~/rpmbuild/BUILD/
cp $name.tar.gz ~/rpmbuild/BUILD/
rpmbuild -bb $spec_tf
rm -f $spec_tf $name.tar.gz
cp ~/rpmbuild/RPMS/$BUILDARCH/* ./


