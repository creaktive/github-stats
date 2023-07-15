#!/usr/bin/env perl
use 5.024;
use experimental qw(signatures);
use warnings qw(all);
no warnings qw(experimental::signatures);

use DBI ();
use FindBin qw($RealBin $RealScript);
use HTTP::Tiny ();
use JSON::PP qw(decode_json);
use List::Util qw(shuffle);
use Scalar::Util qw(looks_like_number);
use Time::Piece;

use constant DATE_SUFFIX => ' 00:00:00';

sub fetch_calendar($login, $year = '') {
    my $url = "https://github.com/users/$login/contributions";
    $url .= "?from=${year}-01-01&to=${year}-12-31" if $year;

    my $response = HTTP::Tiny->new(verify_SSL => 1)->get($url);
    die $response->{reason} unless $response->{success};

    my $parser = qr{
        (?(DEFINE) (?<tag> [^>]*))
        <td (?&tag)
            \b class="ContributionCalendar-day" (?&tag)
            \b data-date="(?<date> \d{4}-\d{2}-\d{2})" (?&tag)
        >
        <span (?&tag)>(?<count> \w+)
    }x;

    my @calendar;
    push @calendar => [
        $+{date} . DATE_SUFFIX,
        looks_like_number($+{count}) ? 0 + $+{count} : 0,
    ] while $response->{content} =~ m{$parser}gosx;
    @calendar = sort { $a->[0] cmp $b->[0] } @calendar;
    pop @calendar unless $year;

    return \@calendar;
}

my @counts = qw(
    date
    repo
    forks
    open_issues
    size
    watchers
);

sub fetch_repos($token) {
    my $api = 'https://api.github.com';
    my $gh = HTTP::Tiny->new(
        default_headers => {
            Accept          => 'application/vnd.github+json',
            Authorization   => "Bearer $token",
        },
        verify_SSL => 1,
    );

    my @stats;
    my %counts;

    my $n = 100;
    for (my $p = 1;; $p++) {
        my $now = gmtime->ymd . DATE_SUFFIX;
        my $response = $gh->get("$api/user/repos?type=public&per_page=$n&page=$p");
        next unless $response->{success};

        my $c = 0;
        for my $repo (decode_json($response->{content})->@*) {
            $repo->{date} = $now;
            $repo->{repo} = $repo->{full_name};
            $counts{$repo->{repo}} = [@$repo{@counts}];
            ++$c;
        }

        last if $n > $c;
    }

    for my $repo (shuffle keys %counts) {
        my $response = $gh->get("$api/repos/$repo/traffic/views");
        next unless $response->{success};

        push @stats => [
            $_->{timestamp} =~ tr{TZ}{ }dr,
            $repo,
            'views',
            $_->{count},
            $_->{uniques},
        ] for decode_json($response->{content})->{views}->@*;
    }

    for my $repo (shuffle keys %counts) {
        my $response = $gh->get("$api/repos/$repo/traffic/clones");
        next unless $response->{success};

        push @stats => [
            $_->{timestamp} =~ tr{TZ}{ }dr,
            $repo,
            'clones',
            $_->{count},
            $_->{uniques},
        ] for decode_json($response->{content})->{clones}->@*;
    }

    @stats = sort {
        ($a->[0] cmp $b->[0]) ||
        ($a->[1] cmp $b->[1]) ||
        ($a->[2] cmp $b->[2])
    } @stats;

    @stats = grep {
        ($_->[0] gt $stats[ 0]->[0]) &&
        ($_->[0] lt $stats[-1]->[0])
    } @stats if @stats;

    return (
        \@stats,
        [sort { $a->[1] cmp $b->[1] } values %counts],
    );
}

sub insert_array($dbh, $table, $headers, $data) {
    $dbh->{AutoCommit} = 0;

    my $insert = sprintf "REPLACE INTO $table (`%s`) VALUES (%s)",
        join('`,`' => @$headers),
        join(',' => ('?') x scalar(@$headers));

    my $sth = $dbh->prepare_cached($insert);
    my $n = 0;
    $n += $sth->execute(@$_) for @$data;
    $sth->finish;

    say STDERR "$n rows inserted into $table";

    $dbh->{AutoCommit} = 1;
    return $n;
}

sub record_data($config, $calendar, $stats, $counts) {
    my $dbh = DBI->connect(
        @$config{qw{db_dsn db_user db_password}},
        { RaiseError => 1 },
    );

    insert_array(
        $dbh,
        $config->{db_table_calendar},
        [qw(date contributions)],
        $calendar,
    );

    insert_array(
        $dbh,
        $config->{db_table_stats},
        [qw(date repo type total uniques)],
        $stats,
    );

    insert_array(
        $dbh,
        $config->{db_table_counts},
        \@counts,
        $counts,
    );

    $dbh->disconnect;
    return;
}

sub main {
    my $config = decode_json do {
        local $/ = undef;
        my $filename = $RealBin . '/'. $RealScript =~ s{\.pl$}{.json}rx;
        open(my $fh, '<:raw', $filename) || die "can't open $filename: $@\n";
        my $data = <$fh>;
        close $fh;
        $data;
    };

    my $calendar = fetch_calendar($config->{github_login});
    # say join "\t", @$_ for @$calendar;

    my ($stats, $counts) = fetch_repos($config->{github_token});
    # say join "\t", @$_ for @$stats;
    # say join "\t", @$_ for @$counts;

    record_data($config, $calendar, $stats, $counts);

    return 0;
}

exit main();

__DATA__
CREATE TABLE `github_calendar` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `date` timestamp NOT NULL,
  `contributions` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `date` (`date`)
);

CREATE TABLE `github_stats` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `date` timestamp NOT NULL,
  `repo` varchar(128) NOT NULL,
  `type` varchar(16) NOT NULL,
  `total` int(11) NOT NULL,
  `uniques` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `date` (`date`,`repo`,`type`)
);

CREATE TABLE `github_counts` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `date` timestamp NOT NULL,
  `repo` varchar(128) NOT NULL,
  `forks` int(11) NOT NULL,
  `open_issues` int(11) NOT NULL,
  `size` int(11) NOT NULL,
  `watchers` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `date` (`date`,`repo`)
);
