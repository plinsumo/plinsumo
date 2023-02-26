#!/usr/bmn/perl -w
use strict;
use warnings;
use Time::Local;
use Net::SMTP;

my $input_day = shift;

my $CATReporterIMID = "DART";
my $CATSubmitterID = "140802";
##SUMO CRD 146310
##GS CRD 361
#3NE4F|X3NE3|3NE31209|3NE41209|3NEF909
my%clientimids = (
	'3NE31209' => '149224:GTSB',
	'3NE41209' => '149224:GTSB',
	'3NE3F909' => '149224:GTSB',
	'EDDY' => '171810:VCPR',
	'WFS' => '126292:WCHV',
	'WFSAVGPX' => '126292:WCHV'
);
my %senderimids = (
	521=>'140802:DARA',
	520=>'140802:DARA',
	181=>,'140802:946',
	627=>'140802:DEGS',
	628=>'140802:DEGS',
	626=>'140802:DEGS',
	183=>'140802:DEGS',
	415=> '140802:DEGS',
	188=> '140802:DEGS',
	190=> '140802:DEGS');

my %exchid = (
	521=>'ISE',
	520=>'ISE',
	627=>'361:GSCS',
	628=>'7897:INCA',
	181=>'BOX',
	626=>'CBOE',
	183=>'CBOE',
	415=>'134284:CMSP',
	188=>'EDGXOP',
	190=>'NYSE');


my %desttypes = (
	521=>'E',
	520=>'E',
	626=>'E',
	183=>'E',
	181=>'E',
	627=>'F',
	628=>'F',
	415=>'F',
	188=>'E',
	190=>'E');

my %mmOrigins = (
	521=>'O',
	520=>'5',
	628=>'5',
	626=>'3',
	183=>'3',
	181=>'X',
	627=>'M',
	415=>'4',
	188=>'N',
	190=>'N');
	
my %firmOrigins = (
	521=>'K',
	520=>'1',
	628=>'3',
	626=>'2',
	183=>'2',
	181=>'F',
	627=>'J',
	415=>'2',
	188=>'J',
	190=>'J');
	
my %customerOrigins = (
	521=>'X',
	520=>'X',
	628=>'X',
	626=>'X',
	183=>'X',
	181=>'X',
	627=>'X',
	415=>'X',
	188=>'X',
	190=>'X');
	
my %sessionids = (
	521=>'IODAR1',
	520=>'IXDAR1',
	181=>'RONIN1',
	626=> 'DART0005',
	183=> 'DART0005',
	188=>'DART0001',
	190=>'NFDEGS01');
	
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
my %reptimes;
my %outs;
my %orig_times;
my %replace_created;
my $sequence = 1; #90650;
my $file_sequence = 9;
my $file_h = &set_file($CATSubmitterID);
my %exch_rej;
my %rom_rej;
my %cancel_rej;

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
my $CATatsField="";
my $CATreceiverIMID=$CATSubmitterID.":".$CATReporterIMID;
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
my $CATunderlying = "";

# temporary variables for 2d
my $CATnetPrice="";
my @CATdeptType = ("O","A","T");
my $CATminQty="";
my @CATaccountHolerType = ("O","A","P");
my @CATaffiliateFlag = ("TRUE","FALSE");
my $CATleavesQty=0;
my $CATinitiator="C";
my $CATrequestTimestamp="";
my @CATsenderType = ("E","O","F"); #line 112
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

my @files = <BOLT*spread.txt>;
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
						#&instr_side($lsyms[$i+2]),
						&instr_side($lsyms[$i],$lsyms[$i+2]),
						abs($lsyms[$i+2])
					);
				}else{
					$one_leg=sprintf("%s@%s@%s@%s@%s@%s|",
						$lsyms[0].$underscore.$n,
						$symbol=$lsyms[$i],
						$optid="",
						"",
						#&instr_side($lsyms[$i+2]),
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
# get leg count for the parents not in the instrument.
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
}
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
		if($lroms[0] eq "E" ) { #new
			if (length($lroms[70])>0) { #there is parent id
			
				if($lroms[35] eq "E" ) { #equity
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


my @ocfiles = <BOLT*spread.txt>;
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

# foreach my $ld (sort keys %leg5_details) {
#     print "$ld => $leg5_details{$ld}\n";
# };

my %col28;
#my $rt;
my %rt;
foreach my $file (@sfiles) {
	open(IN, "<$file") or die "cannot open $file\n";
	while(<IN>) {
		chomp;
		my @lroms = split(/,/);
		if($lroms[0] eq "S"){
			if( $lroms[14] eq "13") {
				my $tout = $outs{$lroms[17]};
				if(defined $tout) {
					print "Second open!! $lroms[17], previous: $tout";
				} else {
					$outs{$lroms[17]} = $lroms[15];
				}
			}
			if( $lroms[14] eq "27" and defined $outs{$lroms[17]}) {
				%col28 =();
				%rt=();
				$col28{$lroms[17]}=$lroms[28];
				$rt{$lroms[28]}=&create_time_str($lroms[52]);
			}elsif($lroms[14] eq "5" and defined $outs{$lroms[17]} and defined $col28{$lroms[17]}){
				$reptimes{$lroms[15]} = $rt{$col28{$lroms[17]}};
# 				print("status = 5 ------------\n");
# 				foreach my $ld (sort keys %col28) {
#      				print "$ld => $col28{$ld}\n";
#  				};
#  				foreach my $ld (sort keys %rt) {
#      				print "$ld => $rt{$ld}\n";
#  				};
				%col28 =();
				%rt=();
			}
			if($lroms[14] eq "8") {
				$outs{$lroms[17]} = $lroms[15];
				if(length($lroms[15])==0) {
					$rom_rej{$lroms[17]} = "true";
				}else{
					$exch_rej{$lroms[17]} = "true";
				}
			}
			if( $lroms[14] eq "14") {
				$cancel_rej{$lroms[17]} = "true";
			}
			if( $lroms[14] eq "20") {
				%col28 =();
				%rt=();
			}
		} elsif ($lroms[0] eq "E") {
			$orig_times{$lroms[17]} = &create_time_str($lroms[52]);
		}
	}
	close(IN);
}


my $romlegs=0;
my $loglegs=0;
my $instrleg=0;
my $nora;

my $origintid;
foreach my $file (@sfiles) {
	open(IN, "<$file") or die "cannot open $file\n";

	while (<IN>) {
		chomp;
		my @romfields = split(/,/);
		my $destRoute = $non_reports{$romfields[41]};
		if(defined($destRoute)) {
			print("Skipping: $romfields[41], route: $romfields[13]\n");
		} else {
			if($romfields[10] eq "X"){
				$origintid = $customerOrigins{$romfields[41]};
			} else {
				$origintid = $firmOrigins{$romfields[41]};
			}
			if(defined $origintid) {
				my $sym = $romfields[5];
				if($sym eq "ZVZZT") {
					print "Test order: $sym, $romfields[41], $romfields[13], $romfields[17] \n";
				} else {
					if($romfields[0] eq "E" and ($romfields[66] eq "3" or $romfields[66] eq "5")) {
						my $cap = $romfields[10];
						if($romfields[66] eq "3") {
							$leg = substr($leg_details{$romfields[17]},0,length($leg_details{$romfields[17]})-1);	
							$nleg = $parents{$romfields[17]};
							$romlegs = $romlegs+1;
						}elsif($romfields[66] eq "5" and defined $leg5_details{$romfields[55]} and not defined $new_legs{$romfields[55]}) {
							$leg =substr($leg5_details{$romfields[55]},0,length($leg5_details{$romfields[55]})-1);	
							$leg =~ s/(Stock)/$romfields[33]/ig;
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
							$loglegs=$loglegs+1;
						}elsif($romfields[66] eq "5" and defined $new_legs{$romfields[55]}){
							$leg = substr($new_legs{$romfields[55]},0,length($new_legs{$romfields[55]})-3);
							$nleg=substr($new_legs{$romfields[55]},length($new_legs{$romfields[55]})-2,1);
							$instrleg=$instrleg+1;
						}
						if($cap eq "X") {
							$nora="N";
							&create_leg_new_order(\@romfields,$leg,$nleg,$nora);
							&create_leg_order_routed(\@romfields,$leg,$nleg,$nora);
						} elsif(not(defined($rom_rej{$romfields[17]}))) {
							$nora="A";
							&create_leg_order_accpted(\@romfields,$leg,$nleg,$nora);
							&create_leg_order_routed(\@romfields,$leg,$nleg,$nora);
						}
					}
					if($romfields[14] eq "26"  and not defined $cancel_rej{$romfields[17]} and ($romfields[66] eq "3" or $romfields[66] eq "5")) {
						&create_leg_order_cancel(\@romfields);
					}
					if($romfields[14] eq "5" and ($romfields[66] eq "3" or $romfields[66] eq "5")) {
						my $foundRep = $replace_created{$romfields[3]};
						&create_leg_order_modify(\@romfields,$leg,$nleg,$nora);
						$replace_created{$romfields[3]} = $romfields[3];
					}
				}
			} else {
				print "Not in the firmOrigins Map: $romfields[41] \n";
			}
		}
	}
	close(IN);
}
# print("romlegs= ",$romlegs," loglegs= ",$loglegs," instrleg= ",$instrleg,"\n");
###########


sub create_leg_new_order {
	my $lroms = shift;
	my $leg = shift;
	my $nleg = shift;
	my $na = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                #1
		$CATerrorROEID,                                                                                #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                   #3 firmROEID
		$CATtype[11],                                                                                  #4
		$CATReporterIMID,                                                                              #5
		&create_time_str($lroms->[52]),                                                                #6 orderKeyDate
		$lroms->[17],                                                                                  #7 orderID
		$CATunderlying,																				   #8
		&create_time_str($lroms->[52]),                                                                #9 eventTimestamp
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
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[11],$na),                             #22 &&handlingInstruction
		&create_firm_id($lroms->[12],$lroms->[46]),                                                    #23 firmDesignatedID
		&getAcctHolerType($lroms->[12]),	##$CATaccountHolerType[2],                                 #24 &&get_account_holder_type
		$CATaffiliateFlag[1],                                                                          #25 &&get_affiliation
		$CATaggregatedOrders,                                                                          #26
		$CATrepresentativeInd,																		   #27
		$CATsolicitationFlag,                                                                        	#28
		$CATRFQID,																						#29
		$nleg,                                                                      	#30 numberofLegs
		$CATpriceType[0],                                                                           	#31 price type
		$leg						#32
	);
	my $lf = $file_h->{"file"};
	print $lf $output;
}

sub create_leg_order_accpted {
	my $lroms = shift;
	my $leg = shift;
	my $nleg = shift;
	my $na = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                    #1
		$CATerrorROEID ,                                                                                   #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                       #3 firmROEID
		$CATtype[12],                                                                                      #4
		$CATReporterIMID,                                                                                  #5
		&create_time_str($lroms->[52]),                                                                    #6 orderKeyDate
		$lroms->[17],                                                                                      #7 orderID
		$CATunderlying,																				       #8 underlying
		&create_time_str($lroms->[52]),                                                                    #9 eventTimestamp
		$CATmanualOrderKeyDate,                                                                            #10
		$CATmanualOrderID,                                                                                 #11
		$CATmanualFlag[0],                                                                                 #12
		$CATelectronicDupFlag,                                                                             #13
		$CATelectronicTimestamp,                                                                           #14
		$CATreceiverIMID,                                                                                  #15
		&get_sender_imid_for_clrid($lroms->[12],$lroms->[22]),                                                          #16 senderIMID
		$CATsenderType[2],                                                                                 #17 &&get_sender_type
		$lroms->[3],                                                                                       #18 routedOrderID
		&checkPrice($lroms->[7], $lroms->[8]),                                                             #19 price
		$lroms->[6],                                                                                       #20 quantity
		$CATminQty,                                                                                        #21 &&get_minqtye
		&convert_type($lroms->[8]),                                                                        #22 orderType
		&checkTif($lroms->[9], $lroms->[52]),                                                              #23
		$CATtradingSession,                                                                                #24
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[12],$na),                                 #25 &&handlingInstruction
		$CATaffiliateFlag[0],                                                                              #26 &&get_affiliation
		$CATsolicitationFlag,                                                                              #27
		$CATpairedOrderID,                                                                                 #28
		$nleg,                                                                      	#29 numberofLegs
		$CATpriceType[0],                                                                           	#30 price type
		$leg,						#31
		$CATdeptType[1] #32
	);
	my $lf = $file_h->{"file"}; 
	print $lf $output;

}

sub create_leg_order_routed {
	my $lroms = shift;
	my $leg = shift;
	my $nleg = shift;
	my $na = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                            #1
		$CATerrorROEID,                                                                                            #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                               #3 firmROEID
		$CATtype[10],                                                                                              #4
		$CATReporterIMID,                                                                                          #5
		&create_time_str($lroms->[52]),                                                                            #6 orderKeyDate
		$lroms->[17],                                                                                              #7 orderID
		$CATunderlying,																				               #8 underlying Optional
		&create_time_str($lroms->[52]),                                                                            #9 eventTimestamp
		$CATmanualFlag[0],                                                                                         #10
		$CATelectronicDupFlag,                                                                                     #11
		$CATelectronicTimestamp,                                                                                   #12
		&get_sender_imid_for_dest($lroms->[13], $lroms->[12]),                                                     #13 senderIMID
		&get_imid_for_dest($lroms->[13]),                                                                          #14 destination
		&get_dest_type($lroms->[13]),                                                                              #15 destinationType
		&getRoutedID($lroms->[17], $lroms->[3]),                                                                   #16 routedOrderID
		&getSessionID($lroms->[13]),                                                                               #17 session
		&checkPrice($lroms->[7], $lroms->[8]),                                                                     #18 price
		$lroms->[6],                                                                                               #19 quantity
		$CATminQty,                                                                                                #20
		&convert_type($lroms->[8]),                                                                                #21 orderType
		&checkTif($lroms->[9], $lroms->[52]),                                                                      #22 timeInForce
		$CATtradingSession,                                                                                        #23 tradingSession
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[10],$na),                                         #24 &&handlingInstruction
		$CATaffiliateFlag[1],                                                                          			   #25
		&checkReject($lroms->[17]),       	                                                        			   #26 routedRejectedFlag
		&getOriginCode($lroms->[10], $lroms->[13]),																	#27 exchOriginCode
		$CATpairedOrderID,                                                         									#28
		$nleg,                                                                      	#30 numberofLegs
		$CATpriceType[0],                                                                           	#31 price type
		$leg						#32
	);
	my $lf = $file_h->{"file"}; 
	print $lf $output;
}

sub create_leg_order_modify {
	my $lroms = shift;
	my $leg = shift;
	my $nleg = shift;
	my $na = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                      #1
		$CATerrorROEID,                                                                                      #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                         #3
		$CATtype[13],                                                                                        #4
		$CATReporterIMID,                                                                                    #5
		&getOriginalTime($lroms->[17], $lroms->[52]),                                                        #6 orderKeyDAte
		$lroms->[17],                                                                                        #7 orderID
		$CATpriorOrderKeyDate,																				 #8 priorOrderKeyDate
		$CATpriorOrderID,																					 #9 priorOrderID
		$CATunderlying,       																				 #10 underlying
		&getModifyTime($lroms->[59], $lroms->[52], $lroms->[15]),                                                          #11 eventTimestamp
		$CATmanualOrderKeyDate,                                                                              #12
		$CATmanualOrderID,                                                                                   #13
		$CATmanualFlag[0],                                                                                   #14 ???
		$CATelectronicDupFlag,                                                                               #15
		$CATelectronicTimestamp,                                                                             #16
		&get_receiver_for_mod($lroms->[12]),		#$CATreceiverIMID,                                       #17
		&get_sender_imid_for_clrid($lroms->[12],$lroms->[22]),                                               #18
		&get_sender_type_for_mod($lroms->[12]),		#$CATsenderType[2],                                      #19 &&get_sender_type, 
		&get_routed_id_for_modify($lroms->[12],$lroms->[3], $lroms->[28], $lroms->[22]),                                   #20
		$CATinitiator,                                                                                       #21
		&checkPrice($lroms->[7], $lroms->[8]),                                                               #22
		$lroms->[6],                                                                                         #23 quantity
		$CATminQty,,                                                                                         #24 &&get_minqty
		$lroms->[49],                                                                                        #25 leaveqty
		&convert_type($lroms->[8]),                                                                          #26 order type
		&checkTif($lroms->[9], $lroms->[52]),                                                                #27
		$CATtradingSession,                                                                                  #28
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[13],$na),                                   #29  &&handlingInstruction
		$CATreservedForFutureUse,	                                                                         #30
		$CATaggregatedOrders,                                                                                #31
		$CATrepresentativeInd,                                                                               #32
		$CATrequestTimestamp,	                                                                             #33
		$nleg,                                                                      	#30 numberofLegs
		$CATpriceType[0],                                                                           	#31 price type
		$leg						#32
    	);
    	my $lf = $file_h->{"file"};
    	print $lf $output;
	
	my $output2 = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                            #1
		$CATerrorROEID,                                                                                            #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                               #3 firmROEID
		$CATtype[10],                                                                                              #4
		$CATReporterIMID,                                                                                          #5
		&getOriginalTime($lroms->[17], $lroms->[52]),                                                                            #6 orderKeyDate
		$lroms->[17],                                                                                              #7 orderID
		$CATunderlying,																				               #8 underlying Optional
		&getModifyTime($lroms->[59], $lroms->[52], $lroms->[15]),                                                                            #9 eventTimestamp
		$CATmanualFlag[0],                                                                                         #10
		$CATelectronicDupFlag,                                                                                     #11
		$CATelectronicTimestamp,                                                                                   #12
		&get_sender_imid_for_dest($lroms->[13], $lroms->[12]),                                                     #13 senderIMID
		&get_imid_for_dest($lroms->[13]),                                                                          #14 destination
		&get_dest_type($lroms->[13]),                                                                              #15 destinationType
		$lroms->[15],                                                                   #16 routedOrderID
		&getSessionID($lroms->[13]),                                                                               #17 session
		&checkPrice($lroms->[7], $lroms->[8]),                                                                     #18 price
		$lroms->[6],                                                                                               #19 quantity
		$CATminQty,                                                                                                #20
		&convert_type($lroms->[8]),                                                                                #21 orderType
		&checkTif($lroms->[9], $lroms->[52]),                                                                      #22 timeInForce
		$CATtradingSession,                                                                                        #23 tradingSession
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[10],$na),                                         #24 &&handlingInstruction
		$CATaffiliateFlag[1],                                                                          			   #25
		&checkReject($lroms->[17]),       	                                                        			   #26 routedRejectedFlag
		&getOriginCode($lroms->[10], $lroms->[13]),																	#27 exchOriginCode
		$CATpairedOrderID,                                                         									#28
		$nleg,                                                                      	#30 numberofLegs
		$CATpriceType[0],                                                                           	#31 price type
		$leg						#32
	);
	print $lf $output2;
}

sub create_leg_order_cancel {
	my $lroms = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                 #1
		$CATerrorROEID ,                                                                                #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                    #3
		$CATtype[14],                                                                                    #4
		$CATReporterIMID,                                                                               #5
		&getOriginalTime($lroms->[17], $lroms->[52]),                                                   #6
		$lroms->[17],                                                                                   #7 orderID
		$CATunderlying,  																				#8 underlying
		&create_time_str($lroms->[52]),                                                                 #9 eventTimestamp
		$CATmanualFlag[0],                                                                              #10 
		$CATelectronicTimestamp,                                                                        #11
		&determine_cancelled_qty($lroms->[6],$lroms->[48]),                                             #12
		$CATleavesQty,                                                                                  #13 &&get_leave_qty
		$CATinitiator,                                                                                  #14
		$CATrequestTimestamp,                                                                           #15 &&get_request_time
	);
	my $lf = $file_h->{"file"}; 
	print $lf $output;
}
######

sub create_firm_id {
	my $account = shift;
	my $cmta = shift;
	$cmta . $account;
}

# sub getModifyTime { #lroms->[59], $lroms->[52], $lroms->[60]
# 	my $myLastID = shift;
# 	my $myDefRomTime = shift;
# 	my $time = $reptimes{$myLastID};
# 	if(defined $time) {
# 		$time;
# 	} else {
# 		print "Could not find sending time for $myLastID \n";
# 		&create_time_str($myDefRomTime);
# 	}
# }

sub getModifyTime {
	my $myLastID = shift;
	my $myDefRomTime = shift;
	my $col15=shift;
	my $time = $reptimes{$myLastID};
	if(defined $time) {
		$time;
	}elsif(substr($myLastID,0,4) ne "ARR=") {
			#print "Could not find Rep sending time for $myLastID \n";
			$time = $reptimes{$col15};
			#if($col15 eq "202209082U48BD"){				print("time ", $time, " col15--",$col15,"--col17-------------------",$myDefRomTime,"\n" );};
			$time;
	}else{
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

sub getOriginCode {
	my $cap = shift;
	my $dest = shift;
	my $dtype =  $desttypes{$dest};
	if(defined $dtype) {
		if($dtype eq "E") {
			if($cap eq "S") {
				my $originCode = $mmOrigins{$dest};
				if(defined $originCode) {
					$originCode;
				} else {
					print "Unable to find MM origin for $dest \n";
					"MM";
				}
			}elsif($cap eq "X"){
				my $originCode = $customerOrigins{$dest};
				if(defined $originCode) {
					$originCode;
				} else {
					print "Unable to find customer origin for $dest, cap = $cap \n";
					$cap;
				}
			} else {
				my $originCode = $firmOrigins{$dest};
				if(defined $originCode) {
					$originCode;
				} else {
					print "Unable to find firm origin for $dest, cap = $cap \n";
					$cap;
				}
			}
		} else {
			"";
		}
	} else {
		"";
	}

}

sub determine_cancelled_qty {
	my $size = shift;
	my $cum = shift;
	my $rez = $size - $cum;
	if($rez < 0) {
		$rez=0;
	}
	$rez;
}

sub create_file {
	my $who = shift;
	my $input_day = shift;
	if(defined $input_day){
		sprintf("%s_DART_%d_BOLTComplex_OrderEvents_%06d.csv", $who, $input_day, $sequence +33);
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
		sprintf("%s_DART_%04d%02d%02d_BOLTComplex_OrderEvents_%06d.csv", $who, $year + 1900, $mon + 1, $mday, $sequence +33);
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

sub create_fore_id {
	my $romtag = shift;
	my $romtime = shift;
	$sequence += 1;
	sprintf("%s_%s-%d", substr($romtime, 0, 8), $romtag, $sequence);

}

sub convert_side {
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

sub get_routed_id_for_modify { #12,3,28,22
	my $clr_acc = shift;
	my $route_id = shift;
	my $om_ex_tag = shift;
	my $txt = shift;
	my $imid;
	if(defined $txt and ($txt eq "EDDY" or substr($txt,0,3) eq "WFS")){
	 	$imid = $clientimids{$txt};
	}else{
		$imid = $clientimids{$clr_acc};
	}
	if(defined $imid) {
		$om_ex_tag;
	} else {
		$route_id;
	}
}

sub get_sender_imid_for_dest { #13,12
	my $dest = shift;
	my $acc = shift;
	my $imid = $senderimids{$dest};
	if(defined $imid) {
		if($acc eq "A4SA1209" || $acc eq "A4SA" || $acc eq "888895") {
			"140802:BGSA";
		} else {
			$imid;
		}
	} else {
		print "Failed to find imid for $dest \n";
		if($acc eq "A4SA1209" || $acc eq "A4SA") {
			"140802:BGSA";
		} else {
			"140802:DEGS";
		}
	}
}

sub get_imid_for_dest {
	my $dest = shift;
	my $exch = $exchid{$dest};
	if(defined $exch) {
		$exch;
	} else {
		print "Failed to find exchange id for $dest \n";
		"";
	}
}

sub get_sender_imid_for_clrid { #12, 22
	my $clr_acc = shift;
	my $txt=shift;
	my $imid;
	if(defined $txt and ($txt eq "EDDY" or substr($txt,0,3) eq "WFS")){
		$imid = $clientimids{$txt};
	}else{
		$imid = $clientimids{$clr_acc};
	}
	if(defined $imid) {
		$imid;
	} elsif ($clr_acc eq "A4SA") {
		"";
	} else {
		"146310:SUMZ";
	}
}

sub get_receiver_for_mod {
	my $acc = shift;
	if($acc eq "A4SA") {
		"";
	} else {
		$CATreceiverIMID;
	}
}

sub get_sender_type_for_mod {
	my $acc = shift;
	if($acc eq "A4SA") {
		"";
	} else {
		$CATsenderType[2];
	}
}


sub getAcctHolerType {
	my $acc = shift;
	if($acc eq "A4SA") {
		"A";
	} else {
		"P";
	}
}

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
		my $strike = ($dstrike * 1000); # + 0.001;
		my $yearMon = substr($fullYearMon,2);
		#my$output =sprintf("%-6s%s%02s%s%08d", $baseSym, $yearMon, $day,$putCall,$strike); 
		my$output =sprintf("%-6s%s%02s%s%08s", $baseSym, $yearMon, $day,$putCall,$strike); 
		$output;
	}	
}

sub checkReject {
	my $id = shift;
	my $e_rej = $exch_rej{$id};
	if(defined $e_rej) {
		$e_rej;
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

sub getRoutedID {
	my $refid = shift;
	my $backup = shift;
	my $orig = $outs{$refid};
	if(defined $orig && $orig ne "") {
		$orig;
	} else {
		$backup;
	}
}

# 57 Execution Instruction, 73 AlgoType S message
sub setHandlingInstructions{
    my $type = shift;
    my $algoFlag = shift;
    my $event = shift;
    my $naind = shift;
    if($type eq "1" and $event eq "MLOA"){
    	"DIR|NH"
	}elsif($event eq "MLOA" or ($naind eq "A" and $event eq "MLOM")){
		if(defined $algoFlag and $algoFlag ne "0") {
            "DIR|ALG";
		}elsif(defined $type){
            if($type eq "P" or $type eq "M" or $type eq "R") {
            	"DIR|PEG";
            }else {
        		"DIR";
            }
		}else{
            "DIR";
        }
    }elsif($naind eq "A" and $event eq "MLOR"){
		"RAR";
    }elsif($naind eq "N"){
        if(defined $algoFlag and $algoFlag ne "0") {
            "ALG";
        } elsif(defined $type) {
            if($type eq "P" or $type eq "M" or $type eq "R") {
            "PEG";
            } else {
        		"";
            }
        }
    }
}

# sub setHandlingInstructions {
#     my $type = shift;
#     my $algoFlag = shift;
#     my $event = shift;
#     if($type eq "1" and $event eq "MLOA"){
#     	"DIR|NH"
#     }elsif($event eq "MLNO" or $event eq "MLOA" or $event eq "MLOM") {
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
#     } else {
# 		if($event eq "MLOR") {
# 			"RAR";
# 		} else {""}
# 	}
# }

sub instr_side
{
	my $sym=shift;
	my $side = shift;
	if(length($sym) eq 21){
		if($side<0) {
			"S";
		} else {
			"B";
		}
	}else{
		if($side<0) {
			"SL";
		} else {
			"B";
		}
	}
};


## change notes:
# 4/4/2022 add "-" between the romtag and the sequence number in sub create_fore_id
# 4/8/2022 new exchange 188 and 190 in the hashmaps
# 4/12/2022 changed the desitnation from EDGX to EDGXOP for 188.
# 5/3/2022 add "EDDY" and "WFS" to clientimid map
# 5/3/2022 updated sub get_routed_id_for_modify and sub get_sender_imid_for_clrid to use the value in field 22
# 5/3/2022 corrected a mistake for MLNO from $CATmanualFlag[1] to $CATmanualFlag[0]
# 5/3/2022 corrected a mistake for MLNO from $CATdeptType[1] to $CATdeptType[2]
# 6/6/2022: added 'WFSAVGPX' => '126292:WCHV' to clientimid hash map and modified subs 'get_routed_id_for_modify' and 'get_sender_imid_for_clrid'
# 20220608: add "DIR|NH" instruction and change "DIR|RAR" to "DIR" in sub setHandlingInstructions
# 20220829: in the sub getSymbol, changed %08d to %08s when formating the optionID. format %08d drops 0.001 from the strik value causing invalid optionId.
# 20220915: modified to handle that the unexpected value (now PHLXO) showed up in field 15 and use field 28 to get the time of status 27 for replace events.
# 20221206: use different converside sub's for the correct value of side in leg details
# 20221207: modified sub instr_side to determine side code for equity by the length of symbol.
# 20230201: modified sub setHandlingInstructions to handle the values differently for MLNO, MLOM and MLOR for new orders.