#!/usr/bin/perl -w
use strict;

sub dewindowfy {
    my ($text) = @_;
    $text =~ s/\xE2\x80[\x98\x99]/'/g;
    $text =~ s/\xE2\x80[\x9C\x9D]/\"/g;
    $text =~ s/\xA0/ /g;
    for my $i (0..length($text)-1) {
	my $o = ord(substr($text,$i,1));
	if ($o > 8000) {
	    if (($o == 8220) or ($o == 8221) or ($o == 8243)) {
		substr($text,$i,1) = "\"";
	    }
	    elsif (($o == 8216) or ($o == 8217)) {
		substr($text,$i,1) = "'";
	    }
	    else {
		substr($text,$i,1) = " ";
	    }
	}
    }
    return $text;
}


