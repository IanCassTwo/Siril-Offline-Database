#!/usr/bin/perl
use POSIX;
use strict;
use warnings;
use DBI;
use Time::HiRes qw(gettimeofday);

#
# This script will reference your local Postgres database and select
# the top 127 brightest stars for each Healpix at level 8. It will
# write the results into a table called 'astrometry' which will be
# referenced by the database build script
#

my $dsn = "DBI:Pg:dbname=stars2;host=localhost;port=5432";
my $username = "stars";
my $password = "stars";

my $dbh = DBI->connect($dsn, $username, $password, { RaiseError => 1, AutoCommit => 1 })
    or die $DBI::errstr;

my $i = 0;
my $batchsize = 127;
print "Copying rows for each healpix pixel\n";
while ($i < 786432) {
    print "Iteration $i\n" if $i % 5000 == 0;

    $dbh->do("
        INSERT INTO astrometry (source_id, healpix8)
        SELECT source_id, healpix8
        FROM STARS
        WHERE healpix8 = ?
        ORDER BY phot_g_mean_mag
        LIMIT $batchsize", undef, $i);
    $i++;
}

print "Done\n";

$dbh->disconnect;

