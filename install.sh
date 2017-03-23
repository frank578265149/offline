#!/usr/bin/env bash
set -e

FWDIR="$(cd `dirname $0`;  pwd)"
DISTDIR="${FWDIR}/dist"
echo "Building binary distribution for XMHT app"
VERSION="1.0"
cd ${FWDIR}

mvn clean install
#mvn dependency:copy-dependencies -DincludeScope=runtime

cd ${FWDIR}
rm -rf ${DISTDIR}
mkdir -p ${DISTDIR}/bin
mkdir -p ${DISTDIR}/conf
mkdir -p ${DISTDIR}/lib
mkdir -p ${DISTDIR}/log
#mkdir -p ${DISTDIR}/project
#mkdir -p ${DISTDIR}/maven

cp ${FWDIR}/bin/* ${DISTDIR}/bin || :
cp ${FWDIR}/conf/* ${DISTDIR}/conf
#cp ${FWDIR}/project/build.properties ${DISTDIR}/project
#cp ${FWDIR}/sbt/sbt ${DISTDIR}/sbt
#cp ${FWDIR}/maven/sbt-launch-lib.bash ${DISTDIR}/sbt
cp ${FWDIR}/log/*   ${DISTDIR}/log
cp ${FWDIR}/target/tracing-source-1.0-*jar        ${DISTDIR}/lib
#cp ${FWDIR}/target/dependency/*.jar     ${DISTDIR}/lib
cp ${FWDIR}/lib/*.jar                   ${DISTDIR}/lib
rm -f ${DISTDIR}/lib/*javadoc.jar
rm -f ${DISTDIR}/lib/*sources.jar
#rm -f ${DISTDIR}/conf/pio-env.sh
#mv ${DISTDIR}/conf/pio-env.sh. ${DISTDIR}/conf/pio-env.sh

touch ${DISTDIR}/RELEASE

TARNAME="TRACING-SOURCE-$VERSION.tar.gz"
TARDIR="TRACING-SOURCE-$VERSION"
cp -r ${DISTDIR} ${TARDIR}

tar zcvf ${TARNAME} ${TARDIR}
rm -rf ${TARDIR}

echo -e "\033[0;32mPredictionIO binary distribution created at $TARNAME\033[0m"