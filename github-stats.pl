#!/usr/bin/env perl
use 5.024;
use experimental qw(signatures);
use warnings qw(all);
no warnings qw(experimental::signatures);

use DBI ();
use FindBin qw($RealBin $RealScript);
use HTTP::Tiny ();
use JSON::PP qw(decode_json);

sub fetch_calendar($login) {
    my $ua = HTTP::Tiny->new(verify_SSL => 1);
    my $response = $ua->get("https://github.com/$login");
    die $response->{reason} unless $response->{success};

    my $parser = qr{
        <rect [^>]+
            \b class="ContributionCalendar-day" [^>]+
            \b data-count="(?<count> \d+)" [^>]+
            \b data-date="(?<date> \d{4}-\d{2}-\d{2})" [^>]*
        >
    }x;

    my @calendar;
    push @calendar => [
        $+{date} . ' 00:00:00',
        0 + $+{count},
    ] while $response->{content} =~ m{$parser}gosx;
    pop @calendar;

    return \@calendar;
}

sub fetch_repos($token) {
    my $api = 'https://api.github.com';
    my $gh = HTTP::Tiny->new(
        default_headers => {
            Accept          => 'application/vnd.github+json',
            Authorization   => "Bearer $token",
        },
        verify_SSL => 1,
    );

    my @repos;
    my @stats;

    my $n = 100;
    for (my $p = 1;; $p++) {
        my $response = $gh->get("$api/user/repos?type=public&per_page=$n&page=$p");
        die $response->{reason} unless $response->{success};

        my @chunk = map { $_->{full_name} } decode_json($response->{content})->@*;
        push @repos => @chunk;
        last if $n > scalar @chunk;
    }

    for my $repo (@repos) {
        my $response = $gh->get("$api/repos/$repo/traffic/views");
        die $response->{reason} unless $response->{success};

        push @stats => [
            $_->{timestamp} =~ tr{TZ}{ }dr,
            $repo,
            'views',
            $_->{count},
            $_->{uniques},
        ] for decode_json($response->{content})->{views}->@*;
    }

    for my $repo (@repos) {
        my $response = $gh->get("$api/repos/$repo/traffic/clones");
        die $response->{reason} unless $response->{success};

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

    return \@stats;
}

sub insert_array($dbh, $table, $headers, $data) {
    $dbh->{AutoCommit} = 0;

    my $insert = sprintf "INSERT IGNORE INTO $table (`%s`) VALUES (%s)",
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

sub record_data($config, $calendar, $stats) {
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

    my $stats = fetch_repos($config->{github_token});
    # say join "\t", @$_ for @$stats;

    record_data($config, $calendar, $stats);

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
