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
my %reptimes;
my %outs;
my %rejects;
my %orig_times;

my %mmAccounts = (
# 	'31XJ1209' => '31XJ1209',
# 	'3JV91209' => '3JV91209',
# 	'3KW61209' => '3KW61209',
# 	'3KW71209' => '3KW71209',
# 	'3JL11209' => '3JL11209',
# 	'3KY01209' => '3KY01209',
# 	'3KX51209' => '3KX51209',
# 	'3KX61209' => '3KX61209',
# 	'3KX71209' => '3KX71209',
# 	'3KX81209' => '3KX81209',
# 	'3KJ11209' => '3KJ11209'
	"XXXX" => 'XXXX'
);

#31XJ1209
my %badmods = (
    '31XJ1209' => '31XJ1209'
);


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
my $CATsenderIMID="146310:SUMZ";
my $CATdestination="140802:DART";
my $CATsession="";
my $CATdupROIDCond="false";
my $CATsenderIMIDMEOM="";
my $CATdestinationType="F";
my $blank="";

# temporary variables for 2d
my $CATnetPrice="";
my @CATdeptType = ("O","A","T","");
my $CATminQty="";
my @CATaccountHolerType = ("O","A","P");
my @CATaffiliateFlag = ("TRUE","FALSE");
my $CATleavesQty=0;
my $CATinitiator="F";
my $CATrequestTimestamp="";
my @CATsenderType = ("E","O","F",""); #line 112
my @CAThandlingInstructions=("DIR","RAR","ALG","PEG","");

my $sequence = 1;
my $file_h = &set_file($CATSubmitterID);

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


my @files = <SUMO*equity.txt>;
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
		my $sym = $romfields[5];
		if($sym eq "ZVZZT") {
			print "Test order: $sym, $romfields[41], $romfields[13], $romfields[17] \n";
		} else {
			if ($romfields[0] eq "E") {
				my $roid = $romfields[3];
				&create_new_order(\@romfields);
				&create_order_routed(\@romfields, &create_time_str($romfields[52]),$romfields[3], $roid);
			}
			if ($romfields[14] eq "26") {
				&create_order_cancel(\@romfields);
			}
			if ($romfields[14] eq "5") { ### and not(defined $badmods{$romfields[12]})) {
				my $roid = &get_routed_id_for_modify_sumo($romfields[3], $romfields[28]);
				&create_order_modify(\@romfields,$roid);
				&create_order_routed(\@romfields, &getModifyTime($romfields[3], $romfields[52]), $roid);
			}
		}
	}
	close(IN);
}

sub create_new_order {
	my $lroms = shift;
    my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
        $CATactionType,                                         			#1
        $CATerrorROEID,                                         			#2
        create_fore_id($lroms->[17], $lroms->[52]),                         #3 firmROEID
        $CATtype[0],                                            			#4
        $CATReporterIMID,                                       			#5
        &create_time_str($lroms->[52]),                         			#6 orderKeyDate
        $lroms->[17],                                           			#7 orderID
        &clean_sym($lroms->[5]),                                            #8 symbol
        &create_time_str($lroms->[52]),                         			#9 eventTimestamp
        $CATmanualFlag,                                         			#10 
        $CATelectronicDupFlag,                                  			#11
        $CATelectronicTimestamp,                                			#12
        $CATmanualOrderKeyDate,                               	  			#13
        $CATmanualOrderID,                                      			#14
        $CATdeptType[2],                                        			#15 &&get_dept_type "T"
        $CATsolicitationFlag,                                   			#16
        $CATRFQID,                                              			#17
        &convert_side($lroms->[4]),                             			#18 side
        &checkPrice($lroms->[7], $lroms->[8]),                  			#19 price
        $lroms->[6],                                            			#20 quantity
        $CATminQty,                                             			#21 &&get_minqty
        &convert_type($lroms->[8]),                             			#22 orderType
        &checkTif($lroms->[9], $lroms->[52]),                  				#23 timeInForce
        &checkSessions($lroms->[13]),                                     	#24 tradingSession
        &setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[0]),  #25
        $CATcustDspIntrFlag,                                    			#26
        $lroms->[12],                                          				#27 firmDestinationID
        &getAccountHolderType($lroms->[12]),                                #28 &&get_account_holder_type
        $CATaffiliateFlag[1],                                   			#29 &&get_affiliation "false"
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
        $CATactionType,                                     		#1
        $CATerrorROEID,                                     		#2
        create_fore_id($lroms->[17], $lroms->[52]),        			#3 firmROEID
        $CATtype[2],                                        		#4
        $CATReporterIMID,                                   		#5
        &getOriginalTime($lroms->[17], $lroms->[52]),               #6 orderKeyDate
        $lroms->[17],                                       		#7 orderID
        &clean_sym($lroms->[5]),                            		#8 symbol
        $CAToriginatingIMID,                                		#9
        $sentTime,                     								#10 eventTimeStap
        $CATmanualFlag,                                     		#11
        $CATelectronicDupFlag,                              		#12
        $CATelectronicTimestamp,                            		#13
        $CATsenderIMID,            									#14
        $CATdestination,                  							#15
        $CATdestinationType,			                       		#16 "F"
        $fixed_roid,					#$lroms->[3],            	#17 routedORderID
        $CATsession,                        						#18 ""
        &convert_side($lroms->[4]),                         		#19 side
        &checkPrice($lroms->[7], $lroms->[8]),              		#20 price
        $lroms->[6],                                        		#21 quatity
        $CATminQty,                                         		#22 &&get_minqty
        &convert_type($lroms->[8]),                         		#23 orderType
        &checkTif($lroms->[9], $lroms->[52]),                		#24 timeInForce
        &checkSessions($lroms->[13]),                       		#25 tradingSession
        $CATaffiliateFlag[1],                               		#26 &&get_affiliation
        $CATisoInd,                                         		#27
        &setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[2]),   #28 &handlingInstruction->&setHandlingInstructions
        &checkReject($lroms->[17]),                             	#29 routeRejectedFlag
        $CATdupROIDCond,                         					#30 use default?
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

sub create_order_modify {
	my $lroms = shift;
	my $fixed_roid = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
        $CATactionType,                                                           #1
        $CATerrorROEID,                                                           #2
        create_fore_id($lroms->[17], $lroms->[52]),                               #3 firmROEID
        $CATtype[4],                                                              #4
        $CATReporterIMID,                                                         #5
        &getOriginalTime($lroms->[17], $lroms->[52]),                             #6 orderKeyDate
        $lroms->[17],                                                             #7 orderID
        &clean_sym($lroms->[5]),                                                  #8 symbol
        $CATpriorOrderKeyDate,                                                    #9
        $CATpriorOrderID,                                                         #10
        $CAToriginatingIMID,                                                      #11
        &getModifyTime($lroms->[3], $lroms->[52]),                               #12 eventTimestamp
        $CATmanualFlag,                                                           #13
        $CATmanualOrderKeyDate,                                                   #14
        $CATmanualOrderID,                                                        #15
        $CATelectronicDupFlag,                                                    #16
        $CATelectronicTimestamp,                                                  #17
        $CATreceiverIMID,                                                         #18
        $CATsenderIMIDMEOM,                                 					  #19
        $CATsenderType[3],                                                        #20 &&get_sender_type
        $fixed_roid,											#$lroms->[3],	  #21 routedOrderID
        $CATrequestTimestamp,                                                     #22
        $CATreserved,                                                          	  #23
        $CATreserved,                                                          	  #24
        $CATreserved,                                                          	  #25
        $CATinitiator,                                                            #26
        &convert_side($lroms->[4]),                                               #27 side
        &checkPrice($lroms->[7], $lroms->[8]),                                    #28 price
        $lroms->[6],                                                              #29 quantity
        $CATminQty,                                                               #30 &&get_minqty
        $lroms->[49],                                                             #31 leaveQty
        &convert_type($lroms->[8]),                                               #32 orderType
        &checkTif($lroms->[9], $lroms->[52]),                                     #33 timeInForce
        &checkSessions($lroms->[13]),                                             #34 trading session
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

sub create_order_cancel {
	my $lroms = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s,%s,%s,%s\n",
        $CATactionType,                                             #1
        $CATerrorROEID,                                             #2
        create_fore_id($lroms->[17], $lroms->[52]),                 #3 firmROEID
        $CATtype[3],                                                #4
        $CATReporterIMID,                                           #5
        &getOriginalTime($lroms->[17], $lroms->[52]),               #6 orderKeyDate
        $lroms->[17],                                               #7 orderID
        &clean_sym($lroms->[5]),                                    #8 symbol
        $CAToriginatingIMID,                                        #9
        &create_time_str($lroms->[52]),                             #10 eventTimestamp
        $CATmanualFlag,                                             #11
        $CATelectronicTimestamp,                                    #12
        &determine_cancelled_qty($lroms->[6],$lroms->[48]),         #13 cancelQty
        $CATleavesQty,                                              #14 &&get_leave_qty;
        $CATinitiator,                                              #15
        $CATseqNum,                                                 #16
        $CATrequestTimestamp,                                       #17 &&get_request_time
        $CATinfoBarierID                                            #18

	);
	my $lf = $file_h->{"file"}; 
	print $lf $output;
}

###############
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

# sub get_routed_id_for_modify {
#     my $clr_acc = shift;
#     my $route_id = shift;
#     my $om_ex_tag = shift;
#     my $imid = $badmods{$clr_acc};
#     if(defined $imid) {
#         $om_ex_tag;
#     } else {
#         $route_id;
#     }
# }

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
# 	sprintf("%s_SUMZ_%04d%02d%02d_SUMOEquity_OrderEvents_%06d.csv", $who, $year + 1900, $mon + 1, $mday, $sequence);
# }

sub create_file {
	my $who = shift;
	my $input_day = shift;
	if(defined $input_day){
		sprintf("%s_SUMZ_%d_SUMOEquity_OrderEvents_%06d.csv", $who, $input_day, $sequence);
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
		sprintf("%s_SUMZ_%04d%02d%02d_SUMOEquity_OrderEvents_%06d.csv", $who, $year + 1900, $mon + 1, $mday, $sequence);
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


sub create_tif_day_date {
	my $utcDate = shift;
	my $local = &create_time_str($utcDate);
	substr($local, 0, 8);
}
sub create_fore_id {
	my $romtag = shift;
	my $romtime = shift;
	my $localRom = &create_time_str($romtime);
	$sequence += 1;
	sprintf("%s_%s-%d", substr($localRom, 0, 8), $romtag, $sequence);
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
sub getAccountHolderType {
	my $id = shift;
	my $mmAcc = $mmAccounts{$id};
	if(defined $mmAcc) {
		"O";
	} else {
		"P"
	}
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
sub checkReject {
	my $id = shift;
	my $rej = $rejects{$id};
	if(defined $rej) {
		$rej;
	} else {
		"false";
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
	my $sym = shift;
	$sym =~ s/_/ /g;
	$sym;
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

sub checkSessions {
	my $dest = shift;
	if($dest eq "517") {
		"ALL";
	} else {
		"REG";
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

sub getRoutedID {
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

	if(defined $algoFlag and $algoFlag ne "0") {
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
#     if($event eq "MENO" or $event eq "MEOA" or $event eq "MEOM") {
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
#     } elsif ($event eq "MEOR") {
#         "RAR";
#     } else {""}
# }

#### change note
# 2/14/2022:
# trunk the decimal digits to 8 for price in sub checkPrice.
# 20220216
# add DIR to MOOA and MOOM event.
# 3/18/2022
# change "A" to "P" in the sub getAccountHolderType.
# 3/22/2022
# empty mmAccounts hashmap with a dummy entry to force the accountHolderType to be "P" for all events and all accounts.
# 5/5/2022 add "-" between the romtag and the sequence number in sub create_fore_id
# 20220608: change "DIR|RAR" to "DIR" in sub setHandlingInstructions
# 1/27/2023: modified sub setHandlingInstructions. No Dir or RAR.