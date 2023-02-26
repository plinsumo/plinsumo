#!/usr/bmn/perl -w
use strict;
use warnings;
use warnings FATAL => 'all';
use Time::Local;
use Net::SMTP;

my $input_day = shift;

my $CATReporterIMID = "GTSB";#"DART";
my $CATSubmitterID = "149224";#"140802";
my $CATreceiverIMID = "DART";

my %desttypes = (
	521=>'E',
	520=>'E',
	626=>'E',
	183=>'E',
	181=>'E',
	627=> 'F',
	628=> 'F',
	415=>'F');
	
	
my %sessionids = (
	521=>'IODAR1',
	520=>'IXDAR1',
	181=>'RONIN1',
	626=> 'DART0005',
	183=> 'DART0005');
	
# non_reports hashmap contains futures destinations
my %non_reports = (
	'790' => '790',
	'795' => '795',
	'496' => '496',
	'590' => '590',
	'497' => '497',
	'591' => '591',
	'791' => '791',
	'796' => '796'
);

my $iscentral = 0;
my %cancels;
my %part_cancel;
my %outs;
my %rejects;
my %orig_times;
my %reptimes;
my $sequence = 1;
my $file_h = &set_file($CATSubmitterID);

my %rom_rej;
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
my $CATnegotiatedTradeFlag="";
my $CATrepresentativeInd="N";
my $CATseqNum = "";
my $CATsenderIMID="149224:GTSB";
my $CATdestination="140802:DART";
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
my $CATreservedForFutureUse="";
my $CATexchOriginCode="";
my $CATdestinationType="F";
my $CATsession="";

# temporary variables for 2d
my $CATnetPrice="";
my @CATdeptType = ("O","A","T");
my $CATminQty="";
my @CATaccountHolderType = ("O","A","P");
my @CATaffiliateFlag = ("TRUE","FALSE");
my $CATleavesQty=0;
my $CATinitiator="F";
my $CATrequestTimestamp="";
my @CATsenderType = ("E","O","F"); #line 112
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


my @files = <GTS*option.txt>;
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
				$cancel_rej{$lroms[17]} = "true";
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
		my $destRoute = $non_reports{$romfields[41]};
		if(defined($destRoute)) {
			print("Skipping: $romfields[41], route: $romfields[13]\n");
		} else {
			if ($romfields[59] =~ m/^S /){
				my @eq_leg = split(/ /, $romfields[59]);
				$romfields[4] = $eq_leg[1];
				$romfields[6] = $eq_leg[2];
			}
			if($romfields[0] eq "E" ) {
				my $roid = $romfields[3];
				&create_new_order(\@romfields);
				&create_order_routed(\@romfields, &create_time_str($romfields[52]), $roid);
			}
			if($romfields[14] eq "26") {
				&create_order_cancel(\@romfields);
			}
			if($romfields[14] eq "5") {
				my $roid = &get_routed_id_for_modify($romfields[59],$romfields[3], $romfields[28]);
				&create_order_modify(\@romfields);
				&create_order_routed(\@romfields, &getModifyTime($romfields[59], $romfields[52]), $roid);
			}
		}
	}
	close(IN);
}


sub create_new_order {
	my $lroms = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                #1
		$CATerrorROEID ,                                                                               #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                   #3
		$CATtype[5],                                                                                   #4
		$CATReporterIMID,                                                                              #5
		&create_time_str($lroms->[52]),                                                                #6
		$lroms->[3],                                                                                  #7
		&getSymbol($lroms->[55],$lroms->[5], $lroms->[30], $lroms->[31], $lroms->[32], $lroms->[62]),  #8
		&create_time_str($lroms->[52]),                                                                #9
		$CATmanualFlag,                                                                                #10 
		$CATmanualOrderKeyDate,                                                                        #11
		$CATmanualOrderID,                                                                             #12
		$CATelectronicDupFlag,                                                                         #13
		$CATelectronicTimestamp,                                                                       #14
		$CATdeptType[2],                                                                               #15 &&get_dept_type
		&convert_side($lroms->[4]),                                                                    #16
		&checkPrice($lroms->[7], $lroms->[8]),                                                         #17
		$lroms->[6],                                                                                   #18
		$CATminQty,                                                                                    #19 &&get_minqty
		&convert_type($lroms->[8]),                                                                    #20
		&checkTif($lroms->[9], $lroms->[52]),                                                          #21
		$CATtradingSession,                                                                            #22
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[5]),                                                                   #23
		$lroms->[12], #&create_firm_id($lroms->[12],$lroms->[46]),                                     #24 &&handlingInstruction
		&getAccountHolderType($lroms->[12]),
		#$CATaccountHolderType[0],                                                                       #25 &&get_account_holder_type
		$CATaffiliateFlag[1],                                                                          #26 &&get_affiliation
		$CATaggregatedOrders,                                                                          #27
		$CATsolicitationFlag,                                                                          #28
		&getOpenClose($lroms->[38]),                                                                   #29
		$CATrepresentativeInd,                                                                         #30
		$CATretiredFieldPosition,                                                                      #31
		$CATRFQID,                                                                                     #32
		$CATnetPrice                                                                                   #33 &&get_net_price
	);
	my $lf = $file_h->{"file"};
	print $lf $output;
}


sub create_order_routed {
	my $lroms = shift;
	my $sentTime = shift;  
    my $fixed_roid = shift;	
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                            #1
		$CATerrorROEID,                                                                                            #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                               #3
		$CATtype[7],                                                                                               #4
		$CATReporterIMID,                                                                                          #5
		&getOriginalTime($lroms->[17], $lroms->[52]),                                                               #6
		$lroms->[3],                                                                                              #7
		&getSymbol($lroms->[55],$lroms->[5], $lroms->[30], $lroms->[31], $lroms->[32], $lroms->[62]),              #8
		$CAToriginatingIMID,                                                                                       #9
		$sentTime,                                                                             					#10
		$CATmanualFlag,                                                                                            #11 need to go with new or accepted?
		$CATelectronicDupFlag,                                                                                     #12
		$CATelectronicTimestamp,                                                                                   #13
		$CATsenderIMID,                                                     									#14 senderIMID
		$CATdestination,                                                                          					#15
		$CATdestinationType,                                                                              		#16 'F'
		$fixed_roid,		#&get_routed_id_for_modify($lroms->[59],$lroms->[3], $lroms->[28]),			#$lroms->[3]                   #17
		$CATsession,	#&getSessionID($lroms->[13]),                                                             #18 ""
		&convert_side($lroms->[4]),                                                                                #19
		&checkPrice($lroms->[7], $lroms->[8]),                                                                     #20
		$lroms->[6],                                                                                               #21 quantity
		$CATminQty,                                                                                                #22 &&get_minqty
		&convert_type($lroms->[8]),                                                                                #23
		&checkTif($lroms->[9], $lroms->[52]),                                                                      #24
		$CATtradingSession,                                                                                        #25
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[7]),                                         #26 &&handlingInstruction
		&checkReject($lroms->[17]),                                                                                     #27
		$CATexchOriginCode,	                                                        #28
		$CATaffiliateFlag[1],                                                                                      #29 &&get_affiliation
		$CATmultiLegInd,                                                                                           #30
		&getOpenClose($lroms->[38]),                                                                               #31
		$CATretiredFieldPosition,                                                                                  #32
		$CATretiredFieldPosition,                                                                                  #33
		$CATpairedOrderID,                                                                                         #34
		$CATnetPrice                                                                                               #35 &&get_net_price
	);
	my $lf = $file_h->{"file"}; 
	print $lf $output;
}


sub create_order_modify {
	my $lroms = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                      #1
		$CATerrorROEID,                                                                                      #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                         #3
		$CATtype[9],                                                                                         #4
		$CATReporterIMID,                                                                                    #5
		&getOriginalTime($lroms->[17], $lroms->[52]),                                                        #6 orderKeyDAte
		$lroms->[3],                                                                                        #7 orderID
		&getSymbol($lroms->[55], $lroms->[5], $lroms->[30], $lroms->[31], $lroms->[32], $lroms->[62]),       #8 OptionID
		$CATpriorOrderKeyDate,                                                                               #9
		$CATpriorOrderID,                                                                                    #10
		$CAToriginatingIMID,                                                                                 #11
		&getModifyTime($lroms->[59], $lroms->[52]),		#&getModifyTime($lroms->[15], $lroms->[52]),         #12 eventTimestamp
		$CATmanualOrderKeyDate,                                                                              #13
		$CATmanualOrderID,                                                                                   #14
		$CATmanualFlag,                                                                                      #15 ???
		$CATelectronicDupFlag,                                                                               #16
		$CATelectronicTimestamp,                                                                             #17
		$CATreceiverIMID,                                                                                    #18
		$CATsenderIMID,                                                            #19
		$CATsenderType[2],                                                                                   #20 &&get_sender_type, 
		&get_routed_id_for_modify($lroms->[59],$lroms->[3],$lroms->[28]),                      #$lroms->[3], #21 
		$CATinitiator,                                                                                       #22
		&convert_side($lroms->[4]),                                                                          #23
		&checkPrice($lroms->[7], $lroms->[8]),                                                               #24
		$lroms->[6],                                                                                         #25 quantity
		$CATminQty,                                                                                        #26 &&get_minqty
		$lroms->[49],                                                                                        #27 leaveqty
		&convert_type($lroms->[8]),                                                                          #28 order type
		&checkTif($lroms->[9], $lroms->[52]),                                                                #29
		$CATtradingSession,                                                                                  #30
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[9]),                                   #31  &&handlingInstruction
		&getOpenClose($lroms->[38]),                                                                         #32
		$CATrequestTimestamp,                                                                                #33 &&get_request_time
		$CATreservedForFutureUse,                                                                            #34
		$CATaggregatedOrders,                                                                                #35
		$CATrepresentativeInd,                                                                               #36
		$CATretiredFieldPosition,                                                                            #37
		$CATretiredFieldPosition,                                                                            #38
		$CATnetPrice                                                                                         #39 &&get_net_price
    	);
    	my $lf = $file_h->{"file"};
    	print $lf $output;
}


sub create_order_cancel {
	my $lroms = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                 #1
		$CATerrorROEID ,                                                                                #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                    #3
		$CATtype[8],                                                                                    #4
		$CATReporterIMID,                                                                               #5
		&getOriginalTime($lroms->[17], $lroms->[52]),                                                   #6
		$lroms->[3],                                                                                   #7 orderID
		&getSymbol($lroms->[55],$lroms->[5], $lroms->[30], $lroms->[31], $lroms->[32], $lroms->[62]),   #8 optionID
		$CAToriginatingIMID,                                                                            #9
		&create_time_str($lroms->[52]),                                                                 #10 eventTimestamp
		$CATmanualFlag,                                                                                 #11 ?
		$CATelectronicTimestamp,                                                                        #12
		&determine_cancelled_qty($lroms->[6],$lroms->[48]),                                             #13
		$CATleavesQty,                                                                                  #14 &&get_leave_qty
		$CATinitiator,                                                                                  #15
		$CATretiredFieldPosition,                                                                       #16
		$CATrequestTimestamp,                                                                           #17 &&get_request_time
	);
	my $lf = $file_h->{"file"}; 
	print $lf $output;
}

sub create_firm_id {
	my $account = shift;
	my $cmta = shift;
	$cmta . $account;
}

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

sub getSessionID {
	my $dest = shift;
	my $exch = $sessionids{$dest};
	if(defined $exch) {
		$exch;
	} else {
		##print "Failed to find exchange id for $dest \n";
		"";
	}
}

sub getOpenClose {
	my $oc = shift;
	if($oc eq "1") {
		"Open";
	} else {
		"Close";
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

# sub getOriginCode {
# 	my $cap = shift;
# 	my $dest = shift;
# 	my $dtype =  $desttypes{$dest};
# 	if(defined $dtype) {
# 		if($dtype eq "E") {
# 			if($cap eq "S") {
# 				my $originCode = $mmOrigins{$dest};
# 				if(defined $originCode) {
# 					$originCode;
# 				} else {
# 					print "Unable to find MM origin for $dest \n";
# 					"MM";
# 				}
# 			} else {
# 				my $originCode = $firmOrigins{$dest};
# 				if(defined $originCode) {
# 					$originCode;
# 				} else {
# 					print "Unable to find firm origin for $dest, cap = $cap \n";
# 					$cap;
# 				}
# 			}
# 		} else {
# 			"";
# 		}
# 	} else {
# 		"";
# 	}
# 
# }

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
# 	my $who = shift;
# 	my $sec;
# 	my $min;
# 	my $hour;
# 	my $mday;
# 	my $mon;
# 	my $year;
# 	my $wday;
# 	my $yday;
# 	my $isdst;
# 	($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime(time);
# 	sprintf("%s_GTSB_%04d%02d%02d_GTSOption_OrderEvents_%06d.csv", $who, $year + 1900, $mon + 1, $mday, $sequence +33);
# }

sub create_file {
	my $who = shift;
	my $input_day = shift;
	if(defined $input_day){
		sprintf("%s_GTSB_%d_GTSOption_OrderEvents_%06d.csv", $who, $input_day, $sequence +33);
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
		sprintf("%s_GTSB_%04d%02d%02d_GTSOption_OrderEvents_%06d.csv", $who, $year + 1900, $mon + 1, $mday, $sequence +33);
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

sub create_header_date { #not being used
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
	$sequence += 1;
	sprintf("%s_%s-%d", substr($romtime, 0, 8), $romtag, $sequence);

}

sub convert_side
{
	my $side = shift;
	if(defined $side) {
		if($side eq "1") {
			"B";
		} elsif($side eq "2") {
			"S";
		} elsif($side eq "5") {
			"S";
		} elsif($side eq "6") {
			"S";
		}
	}
}

sub create_tif_day_date {
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
# sub get_routed_id_for_modify {
# 	my $clr_acc = shift;
# 	my $route_id = shift;
# 	my $om_ex_tag = shift;
# 	my $imid = $clientimids{$clr_acc};
# 	if(defined $imid) {
# 		$om_ex_tag;
# 	} else {
# 		$route_id;
# 	}
# }

# sub get_sender_imid_for_dest {
# 	my $dest = shift;
# 	my $acc = shift;
# 	my $imid = $senderimids{$dest};
# 	if(defined $imid) {
# 		if($acc eq "A4SA1209" || $acc eq "A4SA" || $acc eq "888895") {
# 			"140802:BGSA";
# 		} else {
# 			$imid;
# 		}
# 	} else {
# 		print "Failed to find imid for $dest \n";
# 		if($acc eq "A4SA1209" || $acc eq "A4SA") {
# 			"140802:BGSA";
# 		} else {
# 			"DEGS";
# 		}
# 	}
# }

# sub get_imid_for_dest {
# 	my $dest = shift;
# 	my $exch = $exchid{$dest};
# 	if(defined $exch) {
# 		$exch;
# 	} else {
# 		print "Failed to find exchange id for $dest \n";
# 		"";
# 	}
# }

# sub get_sender_imid_for_clrid {
# 	my $clr_acc = shift;
# 	my $imid = $clientimids{$clr_acc};
# 	if(defined $imid) {
# 		$imid;
# 	} else {
# 		"146310:SUMZ";
# 	}
# }

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

sub getSymbol {
	my $sym = shift;          #55
	my $baseSym = shift;      #5
	my $fullYearMon = shift;  #30
	my $putCall = shift;      #31
	my $dstrike = shift;      #32
	my $day = shift;          #62
	if(length($sym) > 0) {
		$sym =~ s/_/ /g;
		$sym;
	}else {
		my $strike = ($dstrike * 1000);
		my $yearMon = substr($fullYearMon,2);
		my$output =sprintf("%-6s%s%02s%s%08d", $baseSym, $yearMon, $day,$putCall,$strike); 
		$output;
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

sub checkPrice {
	my $price = shift;
	my $type = shift;
	if($type eq "1") {
		"";
	} else {
		my $decimal = index($price, ".")+1;
	 	$price = substr($price,0,$decimal).substr($price,$decimal,8);
		#$price;
	}
}

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
	if($type eq "1") {
		"MKT";
	} else {
		"LMT";
	}
}	

sub getRoutedID { #not being used
	my $refid = shift;
	my $backup = shift;
	my $orig = $outs{$refid};
	if(defined $orig && $orig ne "") {
		$orig;
	} else {
		$backup;
	}
}

sub setHandlingInstructions
{
    my $type = shift;
    my $algoFlag = shift;
    my $event = shift;
    if($type eq "1"){
    	"NH"
    }elsif(defined $algoFlag and $algoFlag ne "0") {
        "ALG";
    }elsif(defined $type and ($type eq "P" or $type eq "M" or $type eq "R")) {
        "PEG";
    } else {
    	""
    }
}

sub getAccountHolderType {
	my $account = shift;
	if($account ne "" and substr($account,0,1) eq "3") {
		"O";
	} else {
		"P"
	}
}

# 57 Execution Instruction, 73 AlgoType S message
# sub setHandlingInstructions
# {
#     my $type = shift;
#     my $algoFlag = shift;
#     my $event = shift;
#     if($type eq "1" and $event eq "MONO"){
#     	"NH"
#     }elsif($event eq "MONO" or $event eq "MOOA" or $event eq "MOOM") {
#         if(defined $algoFlag and $algoFlag ne "0") {
#             "DIR|ALG";
#         } elsif (defined $type) {
#             if($type eq "P" or $type eq "M" or $type eq "R") {
#             	"DIR|PEG";
#             } else {
#         		"DIR|RAR";
#             }
#         } else {
#             "DIR|RAR";
#         }
#     } elsif ($event eq "MOOR") {
#         "RAR";
#     } else {""}
# }

##change note:
#20220214
#added non_reports hashmap and lines to filter out future destinations from be processed for CAT.
# trunk the decimal digits to 8 for price in sub checkPrice.
# 20220216
# add DIR to MOOA and MOOM event.
# 5/5/2022 add "-" between the romtag and the sequence number in sub create_fore_id
# 6/8/2022 add "NH" instruction for MONO event in the sub setHandlingInstructions
# 1/27/2023: modified sub setHandlingInstructions. No Dir or RAR.
# 2/14/2023: add sub getAccountHolderType to determine the accountHolderType