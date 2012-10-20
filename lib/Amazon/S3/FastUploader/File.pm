package Amazon::S3::FastUploader::File;
use strict;
use warnings;
use File::Basename;
use base qw( Class::Accessor );
__PACKAGE__->mk_accessors( qw(local_path target_dir bucket config s3) );

our $VERSION = '0.05';

# stolen from Plack::MIME which is stolen from rack.mime.rb
our $MIME_TYPES = {
    ".3gp"     => "video/3gpp",
    ".a"       => "application/octet-stream",
    ".ai"      => "application/postscript",
    ".aif"     => "audio/x-aiff",
    ".aiff"    => "audio/x-aiff",
    ".asc"     => "application/pgp-signature",
    ".asf"     => "video/x-ms-asf",
    ".asm"     => "text/x-asm",
    ".asx"     => "video/x-ms-asf",
    ".atom"    => "application/atom+xml",
    ".au"      => "audio/basic",
    ".avi"     => "video/x-msvideo",
    ".bat"     => "application/x-msdownload",
    ".bin"     => "application/octet-stream",
    ".bmp"     => "image/bmp",
    ".bz2"     => "application/x-bzip2",
    ".c"       => "text/x-c",
    ".cab"     => "application/vnd.ms-cab-compressed",
    ".cc"      => "text/x-c",
    ".chm"     => "application/vnd.ms-htmlhelp",
    ".class"   => "application/octet-stream",
    ".com"     => "application/x-msdownload",
    ".conf"    => "text/plain",
    ".cpp"     => "text/x-c",
    ".crt"     => "application/x-x509-ca-cert",
    ".css"     => "text/css",
    ".csv"     => "text/csv",
    ".cxx"     => "text/x-c",
    ".deb"     => "application/x-debian-package",
    ".der"     => "application/x-x509-ca-cert",
    ".diff"    => "text/x-diff",
    ".djv"     => "image/vnd.djvu",
    ".djvu"    => "image/vnd.djvu",
    ".dll"     => "application/x-msdownload",
    ".dmg"     => "application/octet-stream",
    ".doc"     => "application/msword",
    ".dot"     => "application/msword",
    ".dtd"     => "application/xml-dtd",
    ".dvi"     => "application/x-dvi",
    ".ear"     => "application/java-archive",
    ".eml"     => "message/rfc822",
    ".eps"     => "application/postscript",
    ".exe"     => "application/x-msdownload",
    ".f"       => "text/x-fortran",
    ".f77"     => "text/x-fortran",
    ".f90"     => "text/x-fortran",
    ".flv"     => "video/x-flv",
    ".for"     => "text/x-fortran",
    ".gem"     => "application/octet-stream",
    ".gemspec" => "text/x-script.ruby",
    ".gif"     => "image/gif",
    ".gz"      => "application/x-gzip",
    ".h"       => "text/x-c",
    ".hh"      => "text/x-c",
    ".htm"     => "text/html",
    ".html"    => "text/html",
    ".ico"     => "image/vnd.microsoft.icon",
    ".ics"     => "text/calendar",
    ".ifb"     => "text/calendar",
    ".iso"     => "application/octet-stream",
    ".jar"     => "application/java-archive",
    ".java"    => "text/x-java-source",
    ".jnlp"    => "application/x-java-jnlp-file",
    ".jpeg"    => "image/jpeg",
    ".jpg"     => "image/jpeg",
    ".js"      => "application/javascript",
    ".json"    => "application/json",
    ".log"     => "text/plain",
    ".m3u"     => "audio/x-mpegurl",
    ".m4v"     => "video/mp4",
    ".man"     => "text/troff",
    ".manifest"=> "text/cache-manifest",
    ".mathml"  => "application/mathml+xml",
    ".mbox"    => "application/mbox",
    ".mdoc"    => "text/troff",
    ".me"      => "text/troff",
    ".mid"     => "audio/midi",
    ".midi"    => "audio/midi",
    ".mime"    => "message/rfc822",
    ".mml"     => "application/mathml+xml",
    ".mng"     => "video/x-mng",
    ".mov"     => "video/quicktime",
    ".mp3"     => "audio/mpeg",
    ".mp4"     => "video/mp4",
    ".mp4v"    => "video/mp4",
    ".mpeg"    => "video/mpeg",
    ".mpg"     => "video/mpeg",
    ".ms"      => "text/troff",
    ".msi"     => "application/x-msdownload",
    ".odp"     => "application/vnd.oasis.opendocument.presentation",
    ".ods"     => "application/vnd.oasis.opendocument.spreadsheet",
    ".odt"     => "application/vnd.oasis.opendocument.text",
    ".ogg"     => "application/ogg",
    ".ogv"     => "video/ogg",
    ".p"       => "text/x-pascal",
    ".pas"     => "text/x-pascal",
    ".pbm"     => "image/x-portable-bitmap",
    ".pdf"     => "application/pdf",
    ".pem"     => "application/x-x509-ca-cert",
    ".pgm"     => "image/x-portable-graymap",
    ".pgp"     => "application/pgp-encrypted",
    ".pkg"     => "application/octet-stream",
    ".pl"      => "text/x-script.perl",
    ".pm"      => "text/x-script.perl-module",
    ".png"     => "image/png",
    ".pnm"     => "image/x-portable-anymap",
    ".ppm"     => "image/x-portable-pixmap",
    ".pps"     => "application/vnd.ms-powerpoint",
    ".ppt"     => "application/vnd.ms-powerpoint",
    ".ps"      => "application/postscript",
    ".psd"     => "image/vnd.adobe.photoshop",
    ".py"      => "text/x-script.python",
    ".qt"      => "video/quicktime",
    ".ra"      => "audio/x-pn-realaudio",
    ".rake"    => "text/x-script.ruby",
    ".ram"     => "audio/x-pn-realaudio",
    ".rar"     => "application/x-rar-compressed",
    ".rb"      => "text/x-script.ruby",
    ".rdf"     => "application/rdf+xml",
    ".roff"    => "text/troff",
    ".rpm"     => "application/x-redhat-package-manager",
    ".rss"     => "application/rss+xml",
    ".rtf"     => "application/rtf",
    ".ru"      => "text/x-script.ruby",
    ".s"       => "text/x-asm",
    ".sgm"     => "text/sgml",
    ".sgml"    => "text/sgml",
    ".sh"      => "application/x-sh",
    ".sig"     => "application/pgp-signature",
    ".snd"     => "audio/basic",
    ".so"      => "application/octet-stream",
    ".svg"     => "image/svg+xml",
    ".svgz"    => "image/svg+xml",
    ".swf"     => "application/x-shockwave-flash",
    ".t"       => "text/troff",
    ".tar"     => "application/x-tar",
    ".tbz"     => "application/x-bzip-compressed-tar",
    ".tcl"     => "application/x-tcl",
    ".tex"     => "application/x-tex",
    ".texi"    => "application/x-texinfo",
    ".texinfo" => "application/x-texinfo",
    ".text"    => "text/plain",
    ".tif"     => "image/tiff",
    ".tiff"    => "image/tiff",
    ".torrent" => "application/x-bittorrent",
    ".tr"      => "text/troff",
    ".txt"     => "text/plain",
    ".vcf"     => "text/x-vcard",
    ".vcs"     => "text/x-vcalendar",
    ".vrml"    => "model/vrml",
    ".war"     => "application/java-archive",
    ".wav"     => "audio/x-wav",
    ".wma"     => "audio/x-ms-wma",
    ".wmv"     => "video/x-ms-wmv",
    ".wmx"     => "video/x-ms-wmx",
    ".wrl"     => "model/vrml",
    ".wsdl"    => "application/wsdl+xml",
    ".xbm"     => "image/x-xbitmap",
    ".xhtml"   => "application/xhtml+xml",
    ".xls"     => "application/vnd.ms-excel",
    ".xml"     => "application/xml",
    ".xpm"     => "image/x-xpixmap",
    ".xsl"     => "application/xml",
    ".xslt"    => "application/xslt+xml",
    ".yaml"    => "text/yaml",
    ".yml"     => "text/yaml",
    ".zip"     => "application/zip",
};

sub upload {
    my $self = shift;

    my $bucket = $self->bucket;
    my $s3 = $self->s3;

    my $opt = {};
    if ($self->config->{encrypt}) {
        $opt->{"x-amz-server-side-encryption"} = 'AES256';
    }

    if ($self->config->{acl_short}) {
        $opt->{acl_short} = $self->config->{acl_short};
    }

    # http://d.hatena.ne.jp/perlcodesample/20080518/1211121650
    my (undef, undef, $ext) = fileparse($self->local_path, qw/\.[^\.]+$/);
    #print $self->local_path, " ext:$ext \n";
    if ($MIME_TYPES->{$ext}) {
        $opt->{content_type} = $MIME_TYPES->{$ext};
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

content_type (mime type) is automatically determined by its extension name.

=head2 from_to

return a string wich shows source filename and target filename

=cut

1;

__END__
