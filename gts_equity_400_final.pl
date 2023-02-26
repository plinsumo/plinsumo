#!/usr/bmn/perl -w
use strict;
use warnings FATAL => 'all';
use Time::Local;
use Net::SMTP;

my $input_day = shift;

my $CATReporterIMID = "GTSB";
my $CATSubmitterID = "149224";

#3NE4F|X3NE3|3NE31209|3NE41209|3NEF909
#my %clientimids = (
#    '3NE31209' => '149224:GTSB',
#    '3NE41209' => =>'149224:GTSB',
#    '3NE3F909' => '149224:GTSB',
#);

my %desttypes = (
    517=>'E',
    146=>'E',
    629=>'F',
    56=> 'F',
    123=>'F',
    369=>'F',
    469=>'F',
    569=>'F',
    158=> 'F');
    
my %citadel = (
    123=>'116797',
    369=>'116797',
    469=>'116797',
    569=>'116797'
);

my $iscentral = 0;
my %reptimes;
my %outs;
my %orig_times;
my %rejects;
my $sequence = 1;
my $file_h = &set_file($CATSubmitterID);
my %cancels;
my %part_cancel;
my %cancel_rej;

# Variables for 2d
my $CATactionType = "NEW";
my $CATerrorROEID = "";
my @CATtype = ("MENO","MEOA","MEOR","MEOC","MEOM","MONO","MOOA","MOOR","MOOC","MOOM","MLOR","MLNO","MLOA","MLOM","MLOC");
my $CATmanualFlag="false";
my $CATelectronicDupFlag="false";
my $CATelectronicTimestamp="";
my $CATmanualOrderKeyDate="";
my $CATmanualOrderID="";
my $CATsolicitationFlag="false";
my $CATRFQID = "";
my $CATtradingSession="REG";
my $CATcustDspIntrFlag="false";
my $CATinfoBarierID="";
my $CATaggregatedOrders="";
my $CATnegotiatedTradeFlag="false";
my $CATrepresentativeInd="N";
my $CATseqNum = "";
my $CATatsField="";
my $CATreceiverIMID="";
my $CATisoInd="NA";
my $CATpairedOrderID = "";
my $CAToriginatingIMID="";
my $CATmultiLegInd="false";
my $CATquoteKeyDate="";
my $CATquoteID="";
my $CATpriorOrderKeyDate="";
my $CATpriorOrderID="";
my $CATreserved="";
my $CATrouteRejectedFlag="false";
my $CATretiredFieldPosition="";
my $CATsenderIMID="149224:GTSB";
my $CATdestination="140802:DART";
my $CATsession="";
my $CATdupROIDCond="false";
my $CATsenderIMIDMEOM="";
my $destinationType = "F";

# temporary variables for 2d
my $CATnetPrice="";
my @CATdeptType = ("O","A","T");
my $CATminQty="";
my @CATaccountHolerType = ("O","A","P");
my @CATaffiliateFlag = ("TRUE","FALSE");
my $CATleavesQty=0;
my $CATinitiator="F";
my $CATrequestTimestamp="";
my @CATsenderType = ("E","O","F",""); #line 112
my @CAThandlingInstructions=("DIR","RAR","ALG","PEG","");

my $tzinfo = `strings /etc/localtime | egrep -o "[CE]ST[56][CE]DT"`;
if (!defined($tzinfo) || length($tzinfo) == 0) {
    print "cannot determine time zone\n";
}
if ($tzinfo =~ "CST6CDT") {
    $iscentral = 1;
    print "Central time\n";
} else {
    print "Eastern time\n";
}
print "$tzinfo\n";


my @files = <GTS*equity.txt>;
my @sfiles =  sort {
    ($a =~ /_(\d+)/)[0] <=> ($b =~ /_(\d+)/)[0]
} @files;
foreach my $file (@sfiles) {
	open(IN, "<$file") or die "cannot open $file\n";
	while(<IN>) {
		chomp;
		my @lroms = split(/,/);
		if($lroms[0] eq "S"){
			if( $lroms[14] eq "26") {
				if($lroms[9] ne "3"){
					if($lroms[9] ne "4"){
						$cancels{$lroms[17]} = &create_time_str($lroms[52]);
						$cancel_rej{$lroms[17]} = "false";
					}
				}
			}  elsif ($lroms[14] eq "4"){
				$outs{$lroms[17]} = "";
			} elsif ($lroms[14] eq "1"){
				$part_cancel{$lroms[17]} = "";
			} elsif ($lroms[14] eq "5"){
				$part_cancel{$lroms[17]} = "";
			}
			if($lroms[14] eq "8") {
				$rejects{$lroms[17]} = "true";
			}
			if( $lroms[14] eq "27") {
				$reptimes{$lroms[15]} = &create_time_str($lroms[52]);
			}
			if( $lroms[14] eq "14") {
				$cancel_rej{$lroms[17]} = "true";  #should be added?
			}
		} elsif ($lroms[0] eq "E") {
			$orig_times{$lroms[17]} = &create_time_str($lroms[52]);
		}
	}
	close(IN);
}
foreach my $file (@sfiles) {
	open(IN, "<$file") or die "cannot open $file\n";

	while (<IN>) {
		chomp;
		my @romfields = split(/,/);
		if ($romfields[59] =~ m/^S /){
			my @eq_leg = split(/ /, $romfields[59]);
			$romfields[4] = $eq_leg[1];
			$romfields[6] = $eq_leg[2];
		}
		if($romfields[0] eq "E" ) {
			my $roid = $romfields[3];
			&create_new_order(\@romfields);
			&create_order_routed(\@romfields, &create_time_str($romfields[52]),$roid);
		}
		if($romfields[14] eq "26" and $cancel_rej{$romfields[17]} eq "false") {
			&create_order_cancel(\@romfields);
		}
		if($romfields[14] eq "5") {
			my $roid = &get_routed_id_for_modify($romfields[59],$romfields[3], $romfields[28]);
			&create_order_modify(\@romfields);
			&create_order_routed(\@romfields, &getModifyTime($romfields[59], $romfields[52]),$roid);
		}
	}
	close(IN);
}

sub create_new_order {
    my $lroms = shift;
    my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
        $CATactionType,                                         #1
        $CATerrorROEID,                                         #2
        create_fore_id($lroms->[17], $lroms->[52]),                          #3 2c uses the current time, not rom time
        $CATtype[0],                                            #4
        $CATReporterIMID,                                       #5
        &create_time_str($lroms->[52]),                         #6 orderKeyDate
        $lroms->[3],                                           #7 orderID
        $lroms->[5],                                            #8 symbol
        &create_time_str($lroms->[52]),                         #9 eventTimestamp
        $CATmanualFlag,                                         #10 
        $CATelectronicDupFlag,                                  #11
        $CATelectronicTimestamp,                                #12
        $CATmanualOrderKeyDate,                                 #13
        $CATmanualOrderID,                                      #14
        $CATdeptType[2],                                        #15 &&get_dept_type "T"
        $CATsolicitationFlag,                                   #16
        $CATRFQID,                                              #17
        &convert_side($lroms->[4]),                             #18
        &checkPrice($lroms->[7], $lroms->[8]),                  #19
        $lroms->[6],                                            #20 quantity
        $CATminQty,                                             #21 &&get_minqty
        &convert_type($lroms->[8]),                             #22 orderType
        &checkTif($lroms->[9], $lroms->[52]),                   #23
        &checkSessions($lroms->[13]),                                     #24
        &setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[0]),   #25
        $CATcustDspIntrFlag,                                    #26
        $lroms->[12],                                           #27
        $CATaccountHolerType[2],                                #28 &&get_account_holder_type "P"
        $CATaffiliateFlag[1],                                   #29 &&get_affiliation "false"
        $CATinfoBarierID,                                       #30
        $CATaggregatedOrders,                                   #31
        $CATnegotiatedTradeFlag,                                #32
        $CATrepresentativeInd,                                  #33
        $CATatsField,                                           #34
        $CATatsField,                                           #35
        $CATatsField,                                           #36
        $CATatsField,                                           #37
        $CATatsField,                                           #38
        $CATatsField,                                           #39
        $CATatsField,                                           #40
        $CATatsField,                                           #41
        $CATatsField,                                           #42
        $CATatsField,                                           #43
        $CATatsField,                                           #44
        $CATatsField,                                           #45
        $CATnetPrice                                            #46 &&get_net_price
    );
    my $lf = $file_h->{"file"};
    print $lf $output;

}


sub create_order_routed {
    my $lroms = shift;
    my $sentTime = shift;
    my $fixed_roid = shift;	
    my $output = sprintf ("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
        $CATactionType,                                     #1
        $CATerrorROEID,                                     #2
        create_fore_id($lroms->[17], $lroms->[52]),        #3
        $CATtype[2],                                        #4
        $CATReporterIMID,                                   #5
        &getOriginalTime($lroms->[17], $lroms->[52]),        #6
        $lroms->[3],                                       #7
        &clean_sym($lroms->[5]),                            #8
        $CAToriginatingIMID,                                #9
        $sentTime,                    						 #10
        $CATmanualFlag,                                     #11
        $CATelectronicDupFlag,                              #12
        $CATelectronicTimestamp,                            #13
        $CATsenderIMID,            							#14
        $CATdestination,                   					#15
        $destinationType,                       			#16 always "F"? check column 13
        $fixed_roid,		#&get_routed_id_for_modify($lroms->[59],$lroms->[3], $lroms->[28]), 		#$lroms->[3],          #17
        $CATsession,                       					 #18 "" by default?
        &convert_side($lroms->[4]),                         #19
        &checkPrice($lroms->[7], $lroms->[8]),              #20
        $lroms->[6],                                        #21
        $CATminQty,                                         #22 &&get_minqty
        &convert_type($lroms->[8]),                         #23
        &checkTif($lroms->[9], $lroms->[52]),                #24
        &checkSessions($lroms->[13]),                       #25
        $CATaffiliateFlag[1],                               #26 &&get_affiliation
        $CATisoInd,                                         #27
        &setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[2]),   #28 &handlingInstruction->&setHandlingInstructions
        &checkReject($lroms->[17]),                         #29
        $CATdupROIDCond,                         			#30
        $CATseqNum,                                         #31
        $CATmultiLegInd,                                    #32
        $CATpairedOrderID,                                  #33
        $CATinfoBarierID,                                   #34
        $CATnetPrice,                                       #35
        $CATquoteKeyDate,                                   #36
        $CATquoteID                                         #37
    );
    my $lf = $file_h->{"file"};
    print $lf $output;
}


sub create_order_cancel {
    my $lroms = shift;
    my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s,%s,%s,%s\n",
        $CATactionType,                                             #1
        $CATerrorROEID,                                             #2
        create_fore_id($lroms->[17], $lroms->[52]),                #3
        $CATtype[3],                                                #4
        $CATReporterIMID,                                           #5
        &getOriginalTime($lroms->[17], $lroms->[52]),               #6
        $lroms->[3],                                               #7
        &clean_sym($lroms->[5]),                                    #8
        $CAToriginatingIMID,                                        #9
        &create_time_str($lroms->[52]),                             #10
        $CATmanualFlag,                                             #11
        $CATelectronicTimestamp,                                    #12
        &determine_cancelled_qty($lroms->[6],$lroms->[48]),         #13
        $CATleavesQty,                                              #14 &&get_leave_qty;
        $CATinitiator,                                              #15
        $CATseqNum,                                                 #16
        $CATrequestTimestamp,                                       #17 &&get_request_time
        $CATinfoBarierID                                            #18
    );
    my $lf = $file_h->{"file"};
    print $lf $output;
}


sub create_order_modify {
    my $lroms = shift;
    my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
        $CATactionType,                                                           #1
        $CATerrorROEID,                                                           #2
        create_fore_id($lroms->[17], $lroms->[52]),                              #3
        $CATtype[4],                                                              #4
        $CATReporterIMID,                                                         #5
        &getOriginalTime($lroms->[17], $lroms->[52]),                             #6
        $lroms->[3],                                                             #7
        &clean_sym($lroms->[5]),                                                  #8
        $CATpriorOrderKeyDate,                                                    #9
        $CATpriorOrderID,                                                         #10
        $CAToriginatingIMID,                                                      #11
        &getModifyTime($lroms->[59], $lroms->[52]),   							#12
        $CATmanualFlag,                                                           #13
        $CATmanualOrderKeyDate,                                                   #14
        $CATmanualOrderID,                                                        #15
        $CATelectronicDupFlag,                                                    #16
        $CATelectronicTimestamp,                                                  #17
        $CATreceiverIMID,                                                         #18
        $CATsenderIMIDMEOM,                                 #19
        $CATsenderType[3],                                                        #20 &&get_sender_type
        &get_routed_id_for_modify($lroms->[59],$lroms->[3], $lroms->[28]),        #$lroms->[3],        #21
        #&get_routed_id_for_modify($lroms->[12],$lroms->[3], $lroms->[28]),        #$lroms->[3],        #21
        $CATrequestTimestamp,                                                     #22
        $CATreserved,                                                          #23
        $CATreserved,                                                          #24
        $CATreserved,                                                          #25
        $CATinitiator,                                                            #26
        &convert_side($lroms->[4]),                                               #27
        &checkPrice($lroms->[7], $lroms->[8]),                                    #28
        $lroms->[6],                                                              #29
        $CATminQty,                                                               #30 &&get_minqty
        $lroms->[49],                                                             #31
        &convert_type($lroms->[8]),                                               #32
        &checkTif($lroms->[9], $lroms->[52]),                                     #33
        &checkSessions($lroms->[13]),                                             #34
        $CATisoInd,                                                               #35
        &setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[4]),        #36 &handlingInstruction->&setHandlingInstructions
        $CATcustDspIntrFlag,                                                      #37
        $CATinfoBarierID,                                                         #38
        $CATaggregatedOrders,                                                     #39
        $CATrepresentativeInd,                                                    #40
        $CATseqNum,                                                               #41
        $CATatsField,                                                             #42
        $CATatsField,                                                             #43
        $CATatsField,                                                             #44
        $CATatsField,                                                             #45
        $CATatsField,                                                             #46
        $CATatsField,                                                             #47
        $CATatsField,                                                             #48
        $CATatsField,                                                             #49
        $CATatsField,                                                             #50
        $CATatsField,                                                             #51
        $CATatsField,                                                             #52
        $CATnetPrice                                                              #53 &&get_net_price
    );
    my $lf = $file_h->{"file"};
    print $lf $output;
}


# sub getModifyTime {
# 	my $myLastID = shift;
# 	my $myDefRomTime = shift;
# 	my $time = $reptimes{$myLastID};
# 	if(defined $time) {
# 		$time;
# 	} else {
# 		print "Could not find Rep sending time for $myLastID \n";
# 		&create_time_str($myDefRomTime);
# 	}
# }
# $myLastID is the rom tag in tag 17
sub getModifyTime {
	my $myLastID = shift;
	my $myDefRomTime = shift;
	my $time = $reptimes{$myLastID};
	if(defined $time) {
		$time;
	} else {
		if(substr($myLastID,0,4) ne "ARR=") {
			print "Could not find Rep sending time for $myLastID \n";
		}
		&create_time_str($myDefRomTime);
	}
}

sub getOriginalTime {
	my $id = shift;
	my $myDefRomTime = shift;
	my $time = $orig_times{$id};
	if(defined $time) {
		$time;
	} else {
		print "Could not find original sending time for $id \n";
		&create_time_str($myDefRomTime);
	}
}

sub checkReject {
	my $id = shift;
	my $rej = $rejects{$id};
	if(defined $rej) {
		$rej;
	} else {
		"false";
	}
}

#sub getSessionID {
#    my $dest = shift;
#    my $exch = $sessionids{$dest};
#    if(defined $exch) {
 #       $exch;
#    } else {
#        "";
#    }
#}

#sub getOpenClose {
#    my $oc = shift;
#    if($oc eq "1") {
 #       "Open";
#    } else {
#        "Close";
 #   }
#}

sub determine_cancelled_qty {
    my $size = shift;
    my $cum = shift;
    my $rez = $size - $cum;
    if($rez < 0) {
        $rez=0;
    }
    $rez;
}

# sub create_file { 
#     my $who = shift;
#     my $sec;
#     my $min;
#     my $hour;
#     my $mday;
#     my $mon;
#     my $year;
#     my $wday;
#     my $yday;
#     my $isdst;
#     ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime(time);
#     sprintf("%s_GTSB_%04d%02d%02d_GTSEquity_OrderEvents_%06d.csv", $who, $year + 1900, $mon + 1, $mday, $sequence);
# }

sub create_file {
	my $who = shift;
	my $input_day = shift;
	if(defined $input_day){
		sprintf("%s_GTSB_%d_GTSEquity_OrderEvents_%06d.csv", $who, $input_day, $sequence);
	}else{
		my $sec;
		my $min;
		my $hour;
		my $mday;
		my $mon;
		my $year;
		my $wday;
		my $yday;
		my $isdst;
		($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime(time);
		sprintf("%s_GTSB_%04d%02d%02d_GTSEquity_OrderEvents_%06d.csv", $who, $year + 1900, $mon + 1, $mday, $sequence);
	}
}

sub set_file {
    my $mpid = shift;
    my $file_name = &create_file($mpid, $input_day);
    my $conn = {};
    my $FILEH;
    open($FILEH, ">", $file_name) or die "cannot open $file_name\n";
    $conn->{"file"} = $FILEH;
    $conn->{"rec"} = 0;
    $conn;
}

sub create_header_date { # not being used?
    my $sec;
    my $min;
    my $hour;
    my $mday;
    my $mon;
    my $year;
    my $wday;
    my $yday;
    my $isdst;
    ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime(time);
    sprintf("%04d%02d%02d", $year + 1900, $mon + 1, $mday);

}

sub create_fore_id {
    my $romtag = shift;
    my $romtime = shift;
    my $localRom = &create_time_str($romtime);
    $sequence += 1;
    sprintf("%s_%s-%d", substr($localRom, 0, 8), $romtag, $sequence);
}

sub convert_side
{
    my $side = shift;
    if(defined $side) {
        if($side eq "1") {
            "B";
        } elsif($side eq "2") {
            "SL";
        } elsif($side eq "5") {
            "SS";
        } elsif($side eq "6") {
            "SX";
        }
    }
}

sub create_tif_day_date { # not being used
    my $utcDate = shift;
    my $local = &create_time_str($utcDate);
    substr($local, 0,8);
}

sub create_time_str {
    my $time_str = shift;
    if ($time_str =~ /(\d{4})(\d{2})(\d{2})-(\d{2}):(\d{2}):(\d{2}).(\d{3})/) {
        my $time = timegm($6, $5, $4, $3, $2 - 1, $1);
        my $milli = $7;
        #print "$milli\n";
        my ($sec, $min, $hour, $mday, $mon, $year) = localtime($time);
        if ($iscentral > 0) {
            $hour += 1;
        }
        sprintf("%04d%02d%02d %02d%02d%02d.%03d",
            $year + 1900, $mon + 1, $mday, $hour, $min, $sec, $milli);
    }
    elsif ($time_str =~ /(\d{4})(\d{2})(\d{2})-(\d{2}):(\d{2}):(\d{2})/) {
        my $time = timegm($6, $5, $4, $3, $2 - 1, $1);
        my ($sec, $min, $hour, $mday, $mon, $year) = localtime($time);
        if ($iscentral > 0) {
            $hour += 1;
        }
        sprintf("%04d%02d%02d %02d%02d%02d.000",
            $year + 1900, $mon + 1, $mday, $hour, $min, $sec);
    }
}

#sub get_sender_imid_for_dest { #not being used
 #   my $dest = shift;
 #   my $imid = $senderimids{$dest};
 #   if(defined $imid) {
 #       $imid;
  #  } else {
  #      print "Failed to find imid for $dest \n";
  #      "DEGS";
  #  }
#}

sub get_routed_id_for_modify { 
   my $myLastID = shift;
   my $route_id = shift;
   my $om_ex_tag = shift;
   my $time = $reptimes{$myLastID};
    if(defined $time) {
       $route_id;
   } else {
		$om_ex_tag
   }
}

#sub get_routed_id_for_modify { # not being used
#    my $clr_acc = shift;
 #   my $route_id = shift;
 #   my $om_ex_tag = shift;
 #   my $imid = $clientimids{$clr_acc};
  #  if(defined $imid) {
  #      $om_ex_tag;
  #  } else {
  #      $route_id;
  #  }
#}

#sub get_sender_imid_for_clrid { #not being used
#    my $clr_acc = shift;
#    my $imid = $clientimids{$clr_acc};
 #   if(defined $imid) {
 #       $imid;
 #   } else {
 #       "146310:SUMZ";
  #  }
#}

#sub get_imid_for_dest { #not being used
#    my $dest = shift;
#    my $exch = $exchid{$dest};
 #   if(defined $exch) {
 #       $exch;
 #   } else {
 #       print "Failed to find exchange id for $dest \n";
 #       "";
 #   }
#}

sub get_dest_type {
    my $dest = shift;
    my $dtype =  $desttypes{$dest};
    if(defined $dtype) {
        $dtype;
    } else {
        print "Failed to find desttype from $dest \n";
        "E";
    }
}

sub getSymbol { #not being used
    my $sym = shift;
    $sym =~ s/_/ /g;
    $sym;
}

sub checkPrice {
    my $price = shift;
    my $type = shift;
    if(($type eq "1") or ($type eq "0")) {
        "";
    } else {
		my $decimal = index($price, ".")+1;
	 	$price = substr($price,0,$decimal).substr($price,$decimal,8);
		#$price;
    }
}

sub checkSessions {
	my $dest = shift;
	if($dest eq "517") {
		"ALL";
	} else {
		"REG";
	}
}

# sub checkTif { # get incorrect date if run on a different date than the order.
# 	my $tif = shift;
# 	if($tif eq "3") {
# 		"IOC";
# 	} else {
# 		my $rtif = "DAY=" . &create_header_date;
# 		$rtif;
# 	}
# }

sub checkTif {
	my $tif = shift;
	my $time = shift;
	if($tif eq "3") {
		"IOC";
	} else {
		my $rtif = "DAY=" . &create_tif_day_date($time);
		$rtif;
	}
}

sub convert_type {
	my $type = shift;
	if($type eq "1" or $type eq "3" or $type eq "5" or $type eq "12") {
		"MKT";
	} else {
		"LMT";
	}
}

sub getRoutedID { #not being used
    my $refid = shift;
    my $backup = shift;
    my $orig = $outs{$refid};
    if(defined $orig and length($orig) > 0) {
        $orig;
    } else {
        $backup;
    }
}

sub clean_sym
{
    my $sym = shift;
    if(defined $sym) {
        $sym =~ s/\// /g;
        $sym =~ s/\./ /g;
    }
    $sym;
}

sub setHandlingInstructions{
    my $type = shift;
    my $algoFlag = shift;

	if($type eq "1"){
		"NH"
	}elsif(defined $algoFlag and $algoFlag ne "0") {
		"ALG"
	}elsif(defined $type and ($type eq "P" or $type eq "M" or $type eq "R")) {
		"PEG"
	}else {
		""
	}
}

# 57 Execution Instruction, 73 AlgoType E message
# sub setHandlingInstructions
# {
#     my $type = shift;
#     my $algoFlag = shift;
#     my $event = shift;
#     if($type eq "1" and $event eq "MENO"){
#     	"NH"
#     }elsif($event eq "MENO" or $event eq "MEOA" or $event eq "MEOM") {
# 			if(defined $algoFlag and $algoFlag ne "0") {
# 				"DIR|ALG";
# 			} elsif (defined $type) {
# 				if($type eq "P" or $type eq "M" or $type eq "R") {
# 					"DIR|PEG";
# 				} else {
# 					"DIR|RAR";
# 				}
# 			} else {
# 				"DIR|RAR";
# 			}
# 	} elsif ($event eq "MEOR") {
# 		"RAR";
# 	} else 
# 	{""}
# }
#### change note
# 2/14/2022:
# trunk the decimal digits to 8 for price in sub checkPrice.
# 20220216
# add DIR to MOOA and MOOM event.
# 5/5/2022 add "-" between the romtag and the sequence number in sub create_fore_id
# 6/8/2022 add "NH" instruction for MENO event in the sub setHandlingInstructions
# 1/27/2023: modified sub setHandlingInstructions. No Dir or RAR.
# 2/14/2023: change $CATaccountHolerType[0] to $CATaccountHolerType[2]