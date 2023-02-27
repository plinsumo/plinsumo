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
	'3KJ11209' => '3KJ11209'
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
my $CATunderlying = "";

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
my @CATpriceType = ("PU", "TC", "TS");


my %leg5_details;
my $leg;
my $legs;
my $nleg;
my $symbol="";
my $optid ="";
#my $OpenClose="OpenClose";
my %parent5;
my $legid;
my %parent_leg;

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

# get the optionID from the debug log
my @files1 = <DART_option*.txt>;
my @sfiles1 =  sort {
	($a =~ /_(\d+)/)[0] <=> ($b =~ /_(\d+)/)[0]
} @files1;

my %optionid;
foreach my $file (@sfiles1) {
	open(IN, "<$file") or die "cannot open $file\n";
	while(<IN>) {
		chomp;
		my @lsyms = split(/,/); # 4 columns
		$optionid{$lsyms[3]} = $lsyms[2];
	}
	close(IN);
};	
# foreach my $id (sort keys %optionid) {
#     print "$id => $optionid{$id}\n";

# get the parent and leg id's from the debug log
my @files2 = <DART_Complex*.txt>;
my @sfiles2 =  sort {
	($a =~ /_(\d+)/)[0] <=> ($b =~ /_(\d+)/)[0]
} @files2;


my $p="";
my $ls="";
foreach my $file (@sfiles2) {
	open(IN, "<$file") or die "cannot open $file\n";
	while(<IN>) {
		chomp;
		my @lcomps = split(/,/);
		if($lcomps[6] ne "A" ) { #this exclude Stock!!!!
			if($lcomps[2] ne $p) {
				if($p ne "") {
					$parent_leg{$p} = $ls; #paired parent and leg ID's
				}
				$p=$lcomps[2];
				$ls=$lcomps[8];
			} else {
				$ls=$ls." ".$lcomps[8];
			}
		}
	}
	close(IN);
};

# foreach my $id (sort keys %parent_leg) {
#     print "$id => $parent_leg{$id}\n";

# get parent id's with openclose flegs for all complex 5 orders.
# this hashmap is used to filter out the complex instruments records from gts

my @files = <SUMO*spread.txt>;
my @sfiles =  sort {
	($a =~ /_(\d+)/)[0] <=> ($b =~ /_(\d+)/)[0]
} @files;

my %parant_id;

foreach my $file (@files) {
	open(IN, "<$file") or die "cannot open $file\n";
	while(<IN>) {
		chomp;
		my @lroms = split(/,/);
		if ($lroms[14] eq "0" and $lroms[66] eq "5") {
			$parant_id{$lroms[55]}=$lroms[38]
		}
	}
	close(IN);
};

# foreach my $id (sort keys %parant_id) {
#     print "$id => $parant_id{$id}\n";
# };


# a hashmap for the equity leg side using the parent id and value in column 59.
my %eleg_side;

foreach my $file (@files) {
	open(IN, "<$file") or die "cannot open $file\n";
	while(<IN>) {
		chomp;
		my @lroms = split(/,/);
		if ($lroms[14] eq "0" and $lroms[66] eq "5" and $lroms[59] ne "") {
			$eleg_side{$lroms[55]}=substr($lroms[59],2,1)
		}
	}
	close(IN);
};


# foreach my $id (sort keys %eleg_side) {
#     print "$id => $eleg_side{$id}\n";
# };

# build legs from the complex instrument file if the id is in ROM raw orders
my %new_legs;
my $underscore ="_";
my @instrs = <"complex_instruments_"$input_day".txt">;
foreach my $instr (@instrs) {
	open(IN, "<$instr") or die "cannot open $instr\n";
	while(<IN>) {
		chomp;
		my @lsyms = split(/,/); 
		$lsyms[0] =~ s/^\s+|\s+$//g;
		if(defined $parant_id{$lsyms[0]}){ 
			my $n_col =scalar @lsyms;
			my $n_legs = (scalar $n_col - 1)/4;
			my $n=0;
			#print($_,"\n");
			#print($n_col," columns and ",$n_legs," legs\n");
			$new_legs{$lsyms[0]}="";
			while($n < $n_legs){
				$n=$n+1;
				my $i=$n*4-2;
				my $one_leg;
				$lsyms[$i]=~s/^\s+|\s+$//g;
				if(length($lsyms[$i])==21){
					$one_leg=sprintf("%s@%s@%s@%s@%s@%s|",
						$lsyms[0].$underscore.$n,
						$symbol="",
						$optid=$lsyms[$i],
						&getOpenClose(substr($parant_id{$lsyms[0]},$n-1,1)),
						&instr_side($lsyms[$i], $lsyms[$i+2]),
						abs($lsyms[$i+2])
					);
				}else{
					$one_leg=sprintf("%s@%s@%s@%s@%s@%s|",
						$lsyms[0].$underscore.$n,
						$symbol=$lsyms[$i],
						$optid="",
						"",
						&convert_side_e($eleg_side{$lsyms[0]}),
						abs($lsyms[$i+2])
					);
				}
				$new_legs{$lsyms[0]}=sprintf("%s%s",
					$new_legs{$lsyms[0]},
					$one_leg
				);
			}
			$new_legs{$lsyms[0]}=sprintf("%s%d\n",$new_legs{$lsyms[0]},$n_legs)
		}
		#print($_,"\n");
	}
	close(IN);
};	

# foreach my $leg (sort keys %new_legs) {
#     print "$leg => $new_legs{$leg}\n";
# };

# get leg counts for complex order 3 using rom raw order messages.
my $same_parent="";
my %parents;
my $i =0;
# parent order hashmap raw data column 70 has the parent order ID same as the ROM_tag in column 17
foreach my $file (@files) {
	open(IN, "<$file") or die "cannot open $file\n";
	while(<IN>) {
		chomp;
		my @lroms = split(/,/);
		if ($lroms[14] eq "0" and $lroms[66] eq "6") {
			if ($lroms[70] ne $same_parent) {
				$i = 1;
				$same_parent=$lroms[70]
			} else {
				$i = $i +1;
			}
		} else {
			$parents{$same_parent}=$i;
		}
	}
	close(IN);
};
# foreach my $p (sort keys %parents) {
#     print "$p => $parents{$p}\n";
# };


my $OpenClose;
my %leg_details;
$same_parent="";
my $leg_side;
# build the legs for complex 3
foreach my $file (@files) {
	open(IN, "<$file") or die "cannot open $file\n";
	while(<IN>) {
		chomp;
		my @lroms = split(/,/);
		if($lroms[0] eq "E" ) {
			if (length($lroms[70])>0) {
				if($lroms[35] eq "E" ) {
					$symbol=$lroms[33];
					$optid ="";
				#$legid="";
					$OpenClose="";
					$leg_side=&convert_side_e($lroms[4])
				} else {
					$optid = &getSymbol($lroms[55],$lroms[5], $lroms[30], $lroms[31], $lroms[32], $lroms[62]),
					$symbol="";
					$OpenClose=&getOpenClose($lroms[38]),
					$leg_side=&convert_side($lroms[4])
				}
				if ($lroms[70] ne $same_parent) {
					$same_parent=$lroms[70];
					$legs = "";
					$i = $i +1;
					$leg = sprintf("%s@%s@%s@%s@%s@%s|",
						$lroms[17],
						$symbol,
						$optid,
						$OpenClose, 
						#&convert_side($lroms[4]),
						$leg_side,
						$lroms[6]);
					$legs = sprintf("%s%s",$legs,$leg);
					print($legs,"\n");
					$leg_details{$lroms[70]} = $legs
				} else {
					$leg = sprintf("%s@%s@%s@%s@%s@%s|",
						$lroms[17],
						$symbol,
						$optid,
						$OpenClose, 
						#&convert_side($lroms[4]),
						$leg_side,
						$lroms[6]);
					$legs = sprintf("%s%s",$legs,$leg);
					$leg_details{$lroms[70]} = $legs
				}
			}
		}
	}
	close(IN);
}


my @ocfiles = <SUMO*spread.txt>;
# parsing the openClose values for complex 5 orders
my %open_close;
my %romtag_55;
foreach my $file (@ocfiles) {
	open(IN, "<$file") or die "cannot open $file\n";
	while(<IN>) {
		chomp;
		my @lroms = split(/,/);
		if($lroms[0] eq "E") {
			if($lroms[66] eq "5") {
				my $clean_op = $lroms[38];
				$clean_op =~ s/[^a-zA-Z0-9,]//g;
				my $m = length($clean_op); 			# replace any characters that are not 0 or 1
				if($m>0) {
					my @leg_id = split(/ /, $parent_leg{$lroms[55]});
					my $n=0;
					while($n<$m) {
						my $oc_key = $lroms[17]."_".$lroms[55]."_".$leg_id[$n];
						$open_close{$oc_key} = substr($clean_op,$n,1);
						$n = $n +1;
					}	
				}
			}
		}
	}
	close(IN);
};
# foreach my $oc (sort keys %open_close) {
#     print "$oc => $open_close{$oc}\n";
# };


my $same_parent5="";
#my $OpenClose;
# parent order hashmap raw data column 70 has the parent order ID same as the ROM_tag in column 17

foreach my $file (@sfiles2) {
	open(IN, "<$file") or die "cannot open $file\n";
	while(<IN>) {
		chomp;
		my @lcomps = split(/,/);
		if(defined $parant_id{$lcomps[2]}){
			if($lcomps[6] eq "S" ) {
				$symbol="stock";
				$optid ="";
				$legid="";
				$OpenClose=""
			} else {
				$optid = $optionid{$lcomps[8]};
				$symbol="";
				$legid=$lcomps[8];
				$OpenClose="OpenClose";
			}
			if ($lcomps[2] ne $same_parent5) {
				$same_parent5=$lcomps[2];
				$legs = "";
				$nleg = "";
			}
			if($lcomps[6] ne "S" and not defined $new_legs{$lcomps[2]}){
				$leg = sprintf("%s@%s@%s@%s@%s@%s|",
					$legid,
					$symbol,
					$optid,
					$OpenClose, 
					$lcomps[10],
					$lcomps[12]);
				$legs = sprintf("%s%s",$legs,$leg);
				$leg5_details{$lcomps[2]} = $legs;
				$parent5{$lcomps[2]} = $lcomps[4]+1;
			}
		}
	}
	close(IN);
};

foreach my $ld (sort keys %leg5_details) {
    print "$ld => $leg5_details{$ld}\n";
};

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
};

my $romlegs=0;
my $loglegs=0;
my $instrleg=0;

foreach my $file (@sfiles) {
	open(IN, "<$file") or die "cannot open $file\n";

	while (<IN>) {
		chomp;
		my @romfields = split(/,/);
		my $destRoute = $non_reports{$romfields[41]};
		if(defined($destRoute)) {
			print("Skipping: $romfields[41], route: $romfields[13]\n");
		} else {
			my $account = $bolt_accounts{$romfields[12]};
			if(defined($account)) {
				print("Skipping Account: $romfields[12], route: $romfields[13]\n");
			} else {
				if ($romfields[0] eq "E" and ($romfields[66] eq "3" or $romfields[66] eq "5")) {
					if($romfields[66] eq "3") {
						$leg = substr($leg_details{$romfields[17]},0,length($leg_details{$romfields[17]})-1);	
						$nleg = $parents{$romfields[17]};
					}elsif($romfields[66] eq "5" and defined $leg5_details{$romfields[55]}) {
						$leg =substr($leg5_details{$romfields[55]},0,length($leg5_details{$romfields[55]})-1);	
						$leg =~ s/Stock/$romfields[33]/ig; #replace "Stock" with the underlying symbol
						if($romfields[5] eq "NDXP") {
							$leg =~ s/(NDX )/NDXP/ig;
						};
 						$nleg = $parent5{$romfields[55]};
						my $i =0;
						my $op = "";
						while($i<$nleg){
							my @leg_id = split(/ /, $parent_leg{$romfields[55]});
							my $oc_key = $romfields[17]."_".$romfields[55]."_".$leg_id[$i];
							if(defined $open_close{$oc_key}) {
								$op = &getOpenClose($open_close{$oc_key});
							}
							$leg =~ s/(OpenClose)/$op/i;
							$i = $i+1;
						}
					}elsif($romfields[66] eq "5" and defined $new_legs{$romfields[55]}){
							$leg = substr($new_legs{$romfields[55]},0,length($new_legs{$romfields[55]})-3);
							$nleg=substr($new_legs{$romfields[55]},length($new_legs{$romfields[55]})-2,1);
							$instrleg=$instrleg+1;
					}
					my $roid = $romfields[3];
					&create_leg_new_order(\@romfields,$leg,$nleg);
					&create_leg_order_routed(\@romfields, &create_time_str($romfields[52]),$leg,$nleg,$roid);
				}
				if ($romfields[14] eq "26" and ($romfields[66] eq "3" or $romfields[66] eq "5")) {
					&create_leg_order_cancel(\@romfields);
				}
				if ($romfields[14] eq "5" and ($romfields[66] eq "3" or $romfields[66] eq "5")) {
					my $roid = &get_routed_id_for_modify_sumo($romfields[3], $romfields[28]);
					&create_leg_order_modify(\@romfields,$leg,$nleg,$roid);
					&create_leg_order_routed(\@romfields, &getModifyTime($romfields[3], $romfields[52]),$leg,$nleg,$roid);

				}
			}
		}
	}
	close(IN);
};

#print("romlegs= ",$romlegs," loglegs= ",$loglegs," instrleg= ",$instrleg,"\n");

sub create_leg_new_order {
	my $lroms = shift;
	my $leg = shift;
	my $nleg = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                #1
		$CATerrorROEID ,                                                                               #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                   #3 firmROEID
		$CATtype[11],                                                                                  #4
		$CATReporterIMID,                                                                              #5
		&create_time_str($lroms->[52]),                                                                #6 orderKeyDate
		$lroms->[17],                                                                                  #7 orderID
		$CATunderlying,  																			   #8 
		&create_time_str($lroms->[52]),                                                                #9 eventTimeStamp
		$CATmanualFlag[0],                                                                             #10 
		$CATmanualOrderKeyDate,                                                                        #11
		$CATmanualOrderID,                                                                             #12
		$CATelectronicDupFlag,                                                                         #13
		$CATelectronicTimestamp,                                                                       #14
		$CATdeptType[2],                                                                               #15 &&get_dept_type
		&checkPrice($lroms->[7], $lroms->[8]),                                                         #16 price
		$lroms->[6],                                                                                   #17 quantity
		$CATminQty,                                                                                    #18 &&get_minqty
		&convert_type($lroms->[8]),                                                                    #19 orderType
		&checkTif($lroms->[9], $lroms->[52]),                                                          #20 timeInForce
		$CATtradingSession,                                                                            #21
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[11]),                            #22 &&handlingInstruction
		$lroms->[12], #&create_firm_id($lroms->[12],$lroms->[46]),                                     #23 firmDesignatedID
		&getAccountHolderType($lroms->[12]),                                                           #24 &&get_account_holder_type
		$CATaffiliateFlag[1],                                                                          #25 &&get_affiliation
		$CATaggregatedOrders,                                                                          #26
		$CATrepresentativeInd,                                                                    	   #27
		$CATsolicitationFlag,                                                                   	   #28
		$CATRFQID,                                                                                     #29
		$nleg,                                                                      	#30 numberofLegs
		$CATpriceType[0],                                                                           	#31 price type
		$leg						#32
	);
	my $lf = $file_h->{"file"}; 
	print $lf $output;

}

sub create_leg_order_routed {
	my $lroms = shift;
	my $sentTime = shift;		
	my $leg = shift;
	my $nleg = shift;
	my $fixed_roid = shift;	
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                            #1
		$CATerrorROEID,                                                                                            #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                               #3 firmROEID
		$CATtype[10],                                                                                              #4
		$CATReporterIMID,                                                                                          #5
		&getOriginalTime($lroms->[17], $lroms->[52]),                                                              #6 orderKeyDate
		$lroms->[17],                                                                                              #7 orderID
		$CATunderlying,  																						   #8
		$sentTime,                                                                            					   #9 eventTimeStamp
		$CATmanualFlag[0],                                                                                         #10
		$CATelectronicDupFlag,                                                                                     #11
		$CATelectronicTimestamp,                                                                                   #12
		$CATsenderIMID,                                                    										   #13 
		$CATdestination,                                                                          				   #14
		$CATdestinationType,                 		                                                               #15 'F'
		$fixed_roid,					#$lroms->[3],                          									   #16 RoutedOrderID
		$CATsession,             				                                                                   #17 session ""
		&checkPrice($lroms->[7], $lroms->[8]),                                                                     #18 price
		$lroms->[6],                                                                                               #19 quantity
		$CATminQty,                                                                                                #20 &&get_minqty
		&convert_type($lroms->[8]),                                                                                #21 orderType
		&checkTif($lroms->[9], $lroms->[52]),                                                                      #22 timeInForce
		$CATtradingSession,                                                                                        #23
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[10]),                                        #24 &&handlingInstruction
		$CATaffiliateFlag[0],                                                                                      #25 &&get_affiliation
		&checkReject($lroms->[17]),                                                                                #26 routeRejectedFlag
		$CATexchOriginCode,	                                                        							   #27
		$CATpairedOrderID,                                                                                         #28
		$nleg,                                                                      							   #29 numberofLegs
		$CATpriceType[0],                                                                           			   #30 priceType
		$leg																									   #31
	);
	my $lf = $file_h->{"file"};
	print $lf $output;
}

sub create_leg_order_modify {
	my $lroms = shift;
	my $leg = shift;
	my $nleg = shift;
	my $fixed_roid = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                      #1
		$CATerrorROEID,                                                                                      #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                         #3 firmROEID
		$CATtype[13],                                                                                        #4
		$CATReporterIMID,                                                                                    #5
		&getOriginalTime($lroms->[17], $lroms->[52]),                                                        #6 orderKeyDate
		$lroms->[17],                                                                                        #7 orderID
		$CATpriorOrderKeyDate,                                                                               #8
		$CATpriorOrderID,                                                                                    #9
		$CATunderlying,																						 #10
		&getModifyTime($lroms->[3], $lroms->[52]),                                                           #11 eventTimestamp
		$CATmanualOrderKeyDate,                                                                              #12
		$CATmanualOrderID,                                                                                   #13
		$CATmanualFlag[0],                                                                                   #14
		$CATelectronicDupFlag,                                                                               #15
		$CATelectronicTimestamp,                                                                             #16
		$CATreceiverIMID,                                                                                    #17 ""
		$blank,                      		                                      							 #18 senderIMID""
		$CATsenderType[3],                                                                                   #19 ""
		$fixed_roid,	#$lroms->[3], #&get_routed_id_for_modify($lroms->[12],$lroms->[3],$lroms->[28]),     #20 routedOrderID "?"
		$CATinitiator,                                                                                       #21 "F"
		&checkPrice($lroms->[7], $lroms->[8]),                                                               #22 price
		$lroms->[6],                                                                                         #23 quantity
		$CATminQty,                                                                                          #24 &&get_minqty
		$lroms->[49],                                                                                        #25 leaveqty
		&convert_type($lroms->[8]),                                                                          #26 orderType
		&checkTif($lroms->[9], $lroms->[52]),                                                                #27 timeInForce
		$CATtradingSession,                                                                                  #28
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[13]),                                  #29  &&handlingInstruction
		$CATreservedForFutureUse,                                                                            #30
		$CATaggregatedOrders,                                                                                #31
		$CATrepresentativeInd,                                                                               #32
		$CATrequestTimestamp,		 																	 	 #33
		$nleg,                                                                      		 				 #34 numberofLegs
		$CATpriceType[0],                                                                           		 #35 priceType
		$leg							 																	 #36
	);
	my $lf = $file_h->{"file"};
	print $lf $output;
}
sub create_leg_order_cancel {
	my $lroms = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                 #1
		$CATerrorROEID ,                                                                                #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                    #3 firmROEID
		$CATtype[14],                                                                                   #4
		$CATReporterIMID,                                                                               #5
		&getOriginalTime($lroms->[17], $lroms->[52]),                                                   #6 orderKeyDate
		$lroms->[17],                                                                                   #7 orderID
		$CATunderlying,   																				#8
		&create_time_str($lroms->[52]),                                                                 #9 eventTimestamp
		$CATmanualFlag[0],                                                                              #10 
		$CATelectronicTimestamp,                                                                        #11
		&determine_cancelled_qty($lroms->[6],$lroms->[48]),                                             #12 cancelQty
		$CATleavesQty,                                                                                  #13 &&get_leave_qty
		$CATinitiator,                                                                                  #14
		$CATrequestTimestamp,                                                                           #15 &&get_request_time
	);
	my $lf = $file_h->{"file"};
	print $lf $output;
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

sub create_file {
	my $who = shift;
	my $input_day = shift;
	if(defined $input_day){
		sprintf("%s_SUMZ_%d_SUMOComplex_OrderEvents_%06d.csv", $who, $input_day, $sequence +33);
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
		sprintf("%s_SUMZ_%04d%02d%02d_SUMOComplex_OrderEvents_%06d.csv", $who, $year + 1900, $mon + 1, $mday, $sequence +33);
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

sub convert_side_e
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
		my$output =sprintf("%-6s%s%02s%s%08s", $baseSym, $yearMon, $day,$putCall,$strike);
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
#     if($event eq "MLNO" or $event eq "MLOA" or $event eq "MLOM") {
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
#     } elsif ($event eq "MLOR") {
#         "RAR";
#     } else {""}
# }

sub instr_side
{
	my $sym=shift;
	my $side = shift;
	if(length($sym) eq 21){
		if($side lt 0) {
			"S";
		} else {
			"B";
		}
	}else{
		if($side lt 0) {
			"SL";
		} else {
			"B";
		}
	}
};

# 3/18/2022
# change "A" to "P" in the sub getAccountHolderType.
# 5/5/2022 add "-" between the romtag and the sequence number in sub create_fore_id
# 20220608: change "DIR|RAR" to "DIR" in sub setHandlingInstructions
# 20220811: reset $same_parent="" before building legs.
# 20220829: in the sub getSymbol, changed %08d to %08s when formating the optionID. format %08d drops 0.001 from the strik value causing invalid optionId.
# 20221207: use different converside sub's for the correct value of side in leg details
# 20221207: modified sub instr_side to determine side code for equity by the length of symbol.
# 1/27/2023: modified sub setHandlingInstructions. No Dir or RAR.