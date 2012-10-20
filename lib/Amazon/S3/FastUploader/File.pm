package Amazon::S3::FastUploader::File;
use strict;
use warnings;
use base qw( Class::Accessor );
__PACKAGE__->mk_accessors( qw(local_path target_dir bucket config s3) );

our $VERSION = '0.04';

sub upload {
    my $self = shift;

    my $bucket = $self->bucket;
    my $s3 = $self->s3;

    my $opt = {};
    if ($self->config->{encrypt}) {
        $opt->{"x-amz-server-side-encryption"} = 'AES256';
    }

    my $count_failed = 0;
    my $max_retry = 5;
    my $is_success = 0;
    
    while (! $is_success && $count_failed < $max_retry) {
        $is_success = $bucket->add_key_filename($self->_remote_key, $self->local_path, $opt) 
                or do { warn $s3->err . ": " . $s3->errstr . "(" . $self->from_to .")" ; $count_failed++; };
        if ($is_success) {
                return 1;
        }
    }

    die "upload failed " . $self->from_to;

}

sub from_to {
    my $self = shift;

    return $self->local_path . " -> " . $self->_remote_key;
}

sub _remote_key {
    my $self = shift;
    my $local_path = $self->{local_path};
    $local_path =~ s|^\./||;
    return  $self->target_dir . $local_path;
}

=head1 NAME

Amazon::S3::FastUploader -  fast uploader to Amazon S3

=head1 SYNOPSIS

This is an internal module of Amazon::S3::FastUploader.

=head1 METHODS

=head2 new

takes a hashref:

local_path: full path to a local file

target_dir: dirname on S3

bucket: a bucket object

config: config option (hashref)

=head2 upload

do uploading

=head2 from_to

return a string wich shows source filename and target filename

=cut

1;

__END__
