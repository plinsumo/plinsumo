#!/usr/bmn/perl -w
use strict;
use warnings FATAL => 'all';
use Time::Local;
use Net::SMTP;

my $input_day = shift;

my $CATReporterIMID = "SUMZ";#"DART";
my $CATSubmitterID = "146310";#"140802";

my $iscentral = 0;
my %cancels;
my %part_cancel;
my %outs;
my %rejects;
my %orig_times;
my %reptimes;

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
my %bolt_accounts = (
	'A4SA' => 'A4SA',
	'A4SA1209' => 'A4SA1209',
	'888895' => '888895'
);

my %mmAccounts = (
	'31XJ1209' => '31XJ1209',
	'3JV91209' => '3JV91209',
	'3KW61209' => '3KW61209',
	'3KW71209' => '3KW71209',
	'3JL11209' => '3JL11209',
	'3KY01209' => '3KY01209',
	'3KX51209' => '3KX51209',
	'3KX61209' => '3KX61209',
	'3KX71209' => '3KX71209',
	'3KX81209' => '3KX81209',
	'3KJ11209' => '3KJ11209',
	'3NR11209' => '3NR11209'
);
my $sequence = 1;
my $file_h = &set_file($CATSubmitterID);


# Variables for 2d
my $CATactionType = "NEW";
my $CATerrorROEID = "";
my @CATtype = ("MENO","MEOA","MEOR","MEOC","MEOM","MONO","MOOA","MOOR","MOOC","MOOM","MLOR","MLNO","MLOA","MLOM","MLOC");
my @CATmanualFlag=("false", "true");
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
my $CATsenderIMID="146310:SUMZ";
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
my $CATreceiverIMID="";
my $blank="";

# temporary variables for 2d
my $CATnetPrice="";
my @CATdeptType = ("O","A","T");
my $CATminQty="";
my @CATaccountHolderType = ("O","A","P");
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


my @files = <SUMO*option.txt>;
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
				$reptimes{$lroms[60]} = &create_time_str($lroms[52]);
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
		my $destRoute = $non_reports{$romfields[41]};
		if(defined($destRoute)) {
			print("Skipping: $romfields[41], route: $romfields[13]\n");
		} else {
			my $account = $bolt_accounts{$romfields[12]};
			if(defined($account)) {
				print("Skipping Account: $romfields[12], route: $romfields[13]\n");
			} else {
				if ($romfields[0] eq "E") {
					my $roid = $romfields[3];
					&create_new_order(\@romfields);
					&create_order_routed(\@romfields, &create_time_str($romfields[52]), $roid);
				}
				if ($romfields[14] eq "26") {
					&create_order_cancel(\@romfields);
				}
				if ($romfields[14] eq "5") {
					#&create_order_modify(\@romfields);
					#&create_order_routed(\@romfields, &getModifyTime($romfields[3], $romfields[52]));
					my $roid = &get_routed_id_for_modify_sumo($romfields[3], $romfields[28]);
					&create_order_modify(\@romfields,$roid);
					&create_order_routed(\@romfields, &getModifyTime($romfields[3], $romfields[52]), $roid);

				}
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
		&create_fore_id($lroms->[17], $lroms->[52]),                                                   #3 firmROEID
		$CATtype[5],                                                                                   #4
		$CATReporterIMID,                                                                              #5
		&create_time_str($lroms->[52]),                                                                #6 orderKeyDate
		$lroms->[17],                                                                                  #7 orderID
		&getSymbol($lroms->[55],$lroms->[5], $lroms->[30], $lroms->[31], $lroms->[32], $lroms->[62]),  #8 optionID
		&create_time_str($lroms->[52]),                                                                #9 eventTimeStamp
		$CATmanualFlag[0],                                                                             #10 
		$CATmanualOrderKeyDate,                                                                        #11
		$CATmanualOrderID,                                                                             #12
		$CATelectronicDupFlag,                                                                         #13
		$CATelectronicTimestamp,                                                                       #14
		$CATdeptType[2],                                                                               #15 &&get_dept_type
		&convert_side($lroms->[4]),                                                                    #16 side
		&checkPrice($lroms->[7], $lroms->[8]),                                                         #17 price
		$lroms->[6],                                                                                   #18 quantity
		$CATminQty,                                                                                    #19 &&get_minqty
		&convert_type($lroms->[8]),                                                                    #20 orderType
		&checkTif($lroms->[9], $lroms->[52]),                                                          #21 timeInForce
		$CATtradingSession,                                                                            #22
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[5]),                             #23 &&handlingInstruction
		$lroms->[12], #&create_firm_id($lroms->[12],$lroms->[46]),                                     #24 firmDesignatedID
		&getAccountHolderType($lroms->[12]),                                                           #25 &&get_account_holder_type
		$CATaffiliateFlag[1],                                                                          #26 &&get_affiliation
		$CATaggregatedOrders,                                                                          #27
		$CATsolicitationFlag,                                                                          #28
		&getOpenClose($lroms->[38]),                                                                   #29 openCloseIndicator
		$CATrepresentativeInd,                                                                         #30
		$CATretiredFieldPosition,                                                                      #31
		$CATRFQID,                                                                                     #32
		$CATnetPrice                                                                   				   #33
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
		&create_fore_id($lroms->[17], $lroms->[52]),                                                               #3 firmROEID
		$CATtype[7],                                                                                               #4
		$CATReporterIMID,                                                                                          #5
		&getOriginalTime($lroms->[17], $lroms->[52]),                                                              #6 orderKeyDate
		$lroms->[17],                                                                                              #7 orderID
		&getSymbol($lroms->[55],$lroms->[5], $lroms->[30], $lroms->[31], $lroms->[32], $lroms->[62]),              #8 optionID
		$CAToriginatingIMID,                                                                                       #9 
		$sentTime,                                                                            					   #10 eventTimeStamp
		$CATmanualFlag[0],                                                                                         #11
		$CATelectronicDupFlag,                                                                                     #12
		$CATelectronicTimestamp,                                                                                   #13
		$CATsenderIMID,                                                    										   #14 
		$CATdestination,                                                                          				   #15
		$CATdestinationType,                 		                                                               #16 'F'
		$fixed_roid,					#$lroms->[3],                                  							   #17 RoutedOrderID
		$CATsession,             				                                                                   #18 session ""
		&convert_side($lroms->[4]),                                                                                #19 side
		&checkPrice($lroms->[7], $lroms->[8]),                                                                     #20 price
		$lroms->[6],                                                                                               #21 quantity
		$CATminQty,                                                                                                #22 &&get_minqty
		&convert_type($lroms->[8]),                                                                                #23 orderType
		&checkTif($lroms->[9], $lroms->[52]),                                                                      #24 timeInForce
		$CATtradingSession,                                                                                        #25
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[7]),                                         #26 &&handlingInstruction
		&checkReject($lroms->[17]),                                                                                #27 routeRejectedFlag
		$CATexchOriginCode,	                                                        							   #28
		$CATaffiliateFlag[0],                                                                                      #29 &&get_affiliation
		$CATmultiLegInd,                                                                                           #30
		&getOpenClose($lroms->[38]),                                                                               #31 openCloseIndicator
		$CATretiredFieldPosition,                                                                                  #32
		$CATretiredFieldPosition,                                                                                  #33
		$CATpairedOrderID,                                                                                         #34
		$CATnetPrice     
	);
	my $lf = $file_h->{"file"};
	print $lf $output;
}

sub create_order_modify {
	my $lroms = shift;
	my $fixed_roid = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                      #1
		$CATerrorROEID,                                                                                      #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                         #3 firmROEID
		$CATtype[9],                                                                                         #4
		$CATReporterIMID,                                                                                    #5
		&getOriginalTime($lroms->[17], $lroms->[52]),                                                        #6 orderKeyDate
		$lroms->[17],                                                                                        #7 orderID
		&getSymbol($lroms->[55], $lroms->[5], $lroms->[30], $lroms->[31], $lroms->[32], $lroms->[62]),       #8 OptionID
		$CATpriorOrderKeyDate,                                                                               #9
		$CATpriorOrderID,                                                                                    #10
		$CAToriginatingIMID,                                                                                 #11
		&getModifyTime($lroms->[3], $lroms->[52]),                                                           #12 eventTimestamp
		$CATmanualOrderKeyDate,                                                                              #13
		$CATmanualOrderID,                                                                                   #14
		$CATmanualFlag[0],                                                                                   #15
		$CATelectronicDupFlag,                                                                               #16
		$CATelectronicTimestamp,                                                                             #17
		$CATreceiverIMID,                                                                                    #18 ""
		$blank,                      		                                      							 #19 senderIMID""
		$CATsenderType[3],                                                                                   #20 ""
		$fixed_roid,	#$lroms->[3], #&get_routed_id_for_modify($lroms->[12],$lroms->[3],$lroms->[28]),        #21 routedOrderID 
		$CATinitiator,                                                                                       #22 "F"
		&convert_side($lroms->[4]),                                                                          #23 side
		&checkPrice($lroms->[7], $lroms->[8]),                                                               #24 price
		$lroms->[6],                                                                                         #25 quantity
		$CATminQty,                                                                                          #26 &&get_minqty
		$lroms->[49],                                                                                        #27 leaveqty
		&convert_type($lroms->[8]),                                                                          #28 orderType
		&checkTif($lroms->[9], $lroms->[52]),                                                                #29 timeInForce
		$CATtradingSession,                                                                                  #30
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[9]),                                   #31  &&handlingInstruction
		&getOpenClose($lroms->[38]),                                                                         #32 openCloseIndicator
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
		&create_fore_id($lroms->[17], $lroms->[52]),                                                    #3 firmROEID
		$CATtype[8],                                                                                    #4
		$CATReporterIMID,                                                                               #5
		&getOriginalTime($lroms->[17], $lroms->[52]),                                                   #6 orderKeyDate
		$lroms->[17],                                                                                   #7 orderID
		&getSymbol($lroms->[55],$lroms->[5], $lroms->[30], $lroms->[31], $lroms->[32], $lroms->[62]),   #8 optionID
		$CAToriginatingIMID,                                                                            #9
		&create_time_str($lroms->[52]),                                                                 #10 eventTimestamp
		$CATmanualFlag[0],                                                                              #11 
		$CATelectronicTimestamp,                                                                        #12
		&determine_cancelled_qty($lroms->[6],$lroms->[48]),                                             #13 cancelQty
		$CATleavesQty,                                                                                  #14 &&get_leave_qty
		$CATinitiator,                                                                                  #15
		$CATretiredFieldPosition,                                                                       #16
		$CATrequestTimestamp,                                                                           #17 &&get_request_time
	);
	my $lf = $file_h->{"file"};
	print $lf $output;
}

#&get_routed_id_for_modify_sumo($lroms->[3], $lroms->[28])
sub get_routed_id_for_modify_sumo {
	my $route_id = shift;
    my $om_ex_tag = shift;
    if(length($route_id) < 5) {
        $om_ex_tag;
    } else {
        $route_id;
    }

}

sub determine_cancelled_qty {
	my $size = shift;
	my $cum = shift;
	my $rez = $size - $cum;
	if($rez < 0) {
		$rez = 0;
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
# 	sprintf("%s_SUMZ_%04d%02d%02d_SUMOOption_OrderEvents_%06d.csv", $who, $year + 1900, $mon + 1, $mday, $sequence +33);
# }

sub create_file {
	my $who = shift;
	my $input_day = shift;
	if(defined $input_day){
		sprintf("%s_SUMZ_%d_SUMOOption_OrderEvents_%06d.csv", $who, $input_day, $sequence +33);
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
		sprintf("%s_SUMZ_%04d%02d%02d_SUMOOption_OrderEvents_%06d.csv", $who, $year + 1900, $mon + 1, $mday, $sequence +33);
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
sub create_header_date {
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


sub getOpenClose {
	my $oc = shift;
	if($oc eq "1") {
		"Open";
	} else {
		"Close";
	}
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
sub getModifyTime {
	my $myLastID = shift;
	my $myDefRomTime = shift;
	my $time = $reptimes{$myLastID};
	if(defined $time) {
		$time;
	} else {
		print "Could not find Rep sending time for $myLastID \n";
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

sub getAccountHolderType {
	my $id = shift;
	my $mmAcc = $mmAccounts{$id};
	if(defined $mmAcc) {
		"O";
	} else {
		"P"
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

sub create_tif_day_date {
	my $utcDate = shift;
	my $local = &create_time_str($utcDate);
	substr($local, 0, 8);
}
sub create_fore_id {
	my $romtag = shift;
	my $romtime = shift;
	$sequence += 1;
	sprintf("%s_%s-%d", substr($romtime, 0, 8), $romtag, $sequence);
}
sub checkTif {
	my $tif = shift;
	my $time = shift;
	if ($tif eq "3") {
		"IOC";
	}
	else {
		my $rtif = "DAY=" . &create_tif_day_date($time);
		$rtif;
	}
}

sub create_time_str {
	my $cwa = shift;
	if ($cwa =~ /(\d{4})(\d{2})(\d{2})-(\d{2}):(\d{2}):(\d{2}).(\d{3})/) {
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
	elsif ($cwa =~ /(\d{4})(\d{2})(\d{2})-(\d{2}):(\d{2}):(\d{2})/) {
		my $time = timegm($6, $5, $4, $3, $2 - 1, $1);
		my ($sec, $min, $hour, $mday, $mon, $year) = localtime($time);
		if ($iscentral > 0) {
			$hour += 1;
		}
		sprintf("%04d%02d%02d %02d%02d%02d.000",
			$year + 1900, $mon + 1, $mday, $hour, $min, $sec);
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

sub convert_type {
	my $type = shift;
	if($type eq "1") {
		"MKT";
	} else {
		"LMT";
	}
}

sub setHandlingInstructions{
    my $type = shift;
    my $algoFlag = shift;

	if(defined $algoFlag and $algoFlag ne "0") {
		"ALG"
	}elsif(defined $type and ($type eq "P" or $type eq "M" or $type eq "R")) {
		"PEG"
	}else {
		""
	}
}

# 57 Execution Instruction, 73 AlgoType S message
# sub setHandlingInstructions
# {
#     my $type = shift;
#     my $algoFlag = shift;
#     my $event = shift;
#     if($event eq "MONO" or $event eq "MOOA" or $event eq "MOOM") {
#         if(defined $algoFlag and $algoFlag ne "0") {
#             "DIR|ALG";
#         } elsif (defined $type) {
#             if($type eq "P" or $type eq "M" or $type eq "R") {
#             	"DIR|PEG";
#             } else {
#         		"DIR";
#             }
#         } else {
#             "DIR";
#         }
#     } elsif ($event eq "MOOR") {
#         "RAR";
#     } else {""}
# }

#### change note
# 2/11/2022:
# trunk the decimal digits to 8 for price in sub checkPrice.
# 20220216
# add DIR to MOOA abd MOOM event
# 3/18/2022
# change "A" to "P" in the sub getAccountHolderType.
##### change notes
# 5/5/2022 add "-" between the romtag and the sequence number in sub create_fore_id
# 20220608: change "DIR|RAR" to "DIR" in sub setHandlingInstructions
# 20220706: add 3NR11209 to the mmAccounts hashmap
# 1/27/2023: modified sub setHandlingInstructions. No Dir or RAR.