#!/usr/bin/perl
use POSIX;
use strict;
use warnings;
use DBI;
use Time::HiRes qw(gettimeofday);

#
# This script iterates every entry in "stars" table in your local postgres
# database and populates the 'healpix8' column. We /could/ work out the
# healpix8 by doing some math on the source_id column but extracting it
# into its own column allows us to easily index which allows quicker access
# later on
#

# Set the healpix level
my $level = 8;

my $dsn = "DBI:Pg:dbname=stars2;host=localhost;port=5432";
my $username = "stars";
my $password = "stars";

my $dbh = DBI->connect($dsn, $username, $password, { RaiseError => 1, AutoCommit => 1 })
    or die $DBI::errstr;

my $i = 0;
my $batchsize = 10000;
my %hmap;
print "Updating batch of rows with NULL healpix...\n";
while (1) {
    #print "Iteration $i, Records " . ($i * $batchsize) . "\n";

    my $rows_to_update = $dbh->selectall_arrayref("
        SELECT source_id
        FROM STARS
        WHERE healpix8 is null
        LIMIT $batchsize", { Slice => {} });
    $i++;

    last unless @$rows_to_update;

    # Record the start time for the iteration
    my $start_time = gettimeofday();

    foreach my $row (@$rows_to_update) {
        my $source_id = $row->{source_id};
        my $healpix8 = floor($source_id / 8796093022208);
        push @{ $hmap{$healpix8} }, $source_id;
    }

    #print("Issuing " . scalar(keys(%hmap)) . " updates\n");
    #$dbh->begin_work;
    foreach my $h (keys %hmap) {
	my $source_ids = $hmap{$h};
	my $placeholders = join(',', ('?') x @$source_ids); # Create placeholders for the IN clause
	$dbh->do("
	    UPDATE STARS
	    SET healpix8 = ?
	    WHERE source_id IN ($placeholders)", undef, $h, @$source_ids);
    }

    #$dbh->commit;


    # Record the end time for the iteration
    my $end_time = gettimeofday();
    my $elapsed_time = $end_time - $start_time;

    # Calculate the number of records processed per second
    my $records_per_second = $batchsize / $elapsed_time;
    print "Iteration $i completed in $elapsed_time seconds, processing $records_per_second records per second.\n";

    %hmap = ();
    last if @$rows_to_update < $batchsize;
}

print "All NULL healpix8 updated.\n";

$dbh->disconnect;

