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
	'3NE41209' => =>'149224:GTSB',
	'3NE3F909' => '149224:GTSB',
	'EDDY' => '171810:VCPR',
	'WFS' => '126292:WCHV',
	'WFSAVGPX' => '126292:WCHV'
);
my %senderimids = (
	521=>'140802:DARA',
	520=>'140802:DARA',
	181=>'140802:946',
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

# exchange: N for mm and J for firm
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
my $sequence = 90650;
my $file_sequence = 9;
my $file_h = &set_file($CATSubmitterID);
my %exch_rej;
my %rom_rej;

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


my @files = <BOLT*option.txt>;
my @sfiles =  sort {
	($a =~ /_(\d+)/)[0] <=> ($b =~ /_(\d+)/)[0]
} @files;

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
			if( $lroms[14] eq "27") {
				$reptimes{$lroms[15]} = &create_time_str($lroms[52]);
			}
			if($lroms[14] eq "8") {
				$outs{$lroms[17]} = $lroms[15];
				if(length($lroms[15])==0) {
					$rom_rej{$lroms[17]} = "true";
				}else{
					$exch_rej{$lroms[17]} = "true";
				}
			}
		} elsif ($lroms[0] eq "E") {
			$orig_times{$lroms[17]} = &create_time_str($lroms[52]);
		}
	}
	close(IN);
}

my $origintid;
my $nora;

foreach my $file (@sfiles) {
	open(IN, "<$file") or die "cannot open $file\n";

	while (<IN>) {
		chomp;
		my @romfields = split(/,/);
		#if ($romfields[59] =~ m/^S /){
		#	my @eq_leg = split(/ /, $romfields[59]);
		#	$romfields[4] = $eq_leg[1];
		#	$romfields[6] = $eq_leg[2];
		#}
		my $destRoute = $non_reports{$romfields[41]};
		if(defined($destRoute)) {
			print("Skipping: $romfields[41], route: $romfields[13]\n");
		} else {
			if($romfields[10] eq "X"){
	#			my $custid = $customerOrigins{$romfields[41]};
				$origintid = $customerOrigins{$romfields[41]};
			} else {
	#			my $imid = $firmOrigins{$romfields[41]};
				$origintid = $firmOrigins{$romfields[41]};
			}
			if(defined $origintid) {
				my $sym = $romfields[5];
				if($sym eq "ZVZZT") {
					print "Test order: $sym, $romfields[41], $romfields[13], $romfields[17] \n";
				} else {
					if($romfields[0] eq "E" ) {
						my $cap = $romfields[10];
						if($cap eq "X") {
							$nora="N";
							&create_new_order(\@romfields,$nora);
							&create_order_routed(\@romfields,$nora);
						} elsif(not(defined($rom_rej{$romfields[17]}))){
							$nora="A";
							&create_order_accpted(\@romfields,$nora);
							&create_order_routed(\@romfields,$nora);
						}
					}
					if($romfields[14] eq "26") {
						&create_order_cancel(\@romfields);
					}
					if($romfields[14] eq "5") {
						my $foundRep = $replace_created{$romfields[3]};
						if(defined $foundRep) {
							print("Already handled replace for $foundRep \n");
						} else {
							&create_order_modify(\@romfields,$nora);
							$replace_created{$romfields[3]} = $romfields[3];
						}
					}
				}
			} else {
				print "Not in the firmOrigins Map: $romfields[41] \n";
			}
		}
	}
	close(IN);
}


sub create_new_order {
	my $lroms = shift;
	my $na = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                #1
		$CATerrorROEID ,                                                                               #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                   #3
		$CATtype[5],                                                                                   #4
		$CATReporterIMID,                                                                              #5
		&create_time_str($lroms->[52]),                                                                #6
		$lroms->[17],                                                                                  #7
		&getSymbol($lroms->[55],$lroms->[5], $lroms->[30], $lroms->[31], $lroms->[32], $lroms->[62]),  #8
		&create_time_str($lroms->[52]),                                                                #9
		$CATmanualFlag,                                                                                #10 
		$CATmanualOrderKeyDate,                                                                        #11
		$CATmanualOrderID,                                                                             #12
		$CATelectronicDupFlag,                                                                         #13
		$CATelectronicTimestamp,                                                                       #14
		$CATdeptType[1],                                                                               #15 &&get_dept_type
		&convert_side($lroms->[4]),                                                                    #16
		&checkPrice($lroms->[7], $lroms->[8]),                                                         #17
		$lroms->[6],                                                                                   #18
		$CATminQty,                                                                                    #19 &&get_minqty
		&convert_type($lroms->[8]),                                                                    #20
		&checkTif($lroms->[9], $lroms->[52]),                                                          #21
		$CATtradingSession,                                                                            #22
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[5],$na),                                                                   #23
		&create_firm_id($lroms->[12],$lroms->[46]),                                                    #24 &&handlingInstruction
		&getAcctHolerType($lroms->[12]),	##$CATaccountHolerType[2],                                 #25 &&get_account_holder_type
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


sub create_order_accpted {
	my $lroms = shift;
	my $na = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                    #1
		$CATerrorROEID ,                                                                                   #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                       #3
		$CATtype[6],                                                                                       #4
		$CATReporterIMID,                                                                                  #5
		&create_time_str($lroms->[52]),                                                                    #6
		$lroms->[17],                                                                                      #7 orderID
		&getSymbol($lroms->[55],$lroms->[5], $lroms->[30], $lroms->[31], $lroms->[32], $lroms->[62]),      #8 optionId?
		&create_time_str($lroms->[52]),                                                                    #9
		$CATmanualOrderKeyDate,                                                                            #10
		$CATmanualOrderID,                                                                                 #11
		$CATmanualFlag,                                                                                    #12
		$CATelectronicDupFlag,                                                                             #13
		$CATelectronicTimestamp,                                                                           #14
		$CATreceiverIMID,                                                                                  #15
		&get_sender_imid_for_clrid($lroms->[12],$lroms->[22]),                                                          #16
		$CATsenderType[2],                                                                                 #17 &&get_sender_type
		$lroms->[3],                                                                                       #18
		$CATdeptType[1],                                                                                   #19 &&get_dept_type
		&convert_side($lroms->[4]),                                                                        #20
		&checkPrice($lroms->[7], $lroms->[8]),                                                             #21
		$lroms->[6],                                                                                       #22
		$CATminQty,                                                                                        #23 &&get_minqtye
		&convert_type($lroms->[8]),                                                                        #24
		&checkTif($lroms->[9], $lroms->[52]),                                                              #25
		$CATtradingSession,                                                                                #26
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[6],$na),                                 #27 &&handlingInstruction
		$CATaffiliateFlag[0],                                                                              #28 &&get_affiliation
		$CATsolicitationFlag,                                                                              #29
		$CATpairedOrderID,                                                                                 #30
		&getOpenClose($lroms->[38]),                                                                       #31
		$CATretiredFieldPosition,                                                                          #32
		$CATretiredFieldPosition,                                                                          #33
		$CATnetPrice                                                                                       #34 &&get_net_price
	);
	my $lf = $file_h->{"file"}; 
	print $lf $output;

}


sub create_order_routed {
	my $lroms = shift;
	my $na = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                            #1
		$CATerrorROEID,                                                                                            #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                               #3
		$CATtype[7],                                                                                               #4
		$CATReporterIMID,                                                                                          #5
		&create_time_str($lroms->[52]),                                                                            #6
		$lroms->[17],                                                                                              #7
		&getSymbol($lroms->[55],$lroms->[5], $lroms->[30], $lroms->[31], $lroms->[32], $lroms->[62]),              #8
		$CAToriginatingIMID,                                                                                       #9
		&create_time_str($lroms->[52]),                                                                            #10
		$CATmanualFlag,                                                                                            #11 need to go with new or accepted?
		$CATelectronicDupFlag,                                                                                     #12
		$CATelectronicTimestamp,                                                                                   #13
		&get_sender_imid_for_dest($lroms->[13], $lroms->[12]),                                                     #14 senderIMID
		&get_imid_for_dest($lroms->[13]),                                                                          #15
		&get_dest_type($lroms->[13]),                                                                              #16
		&getRoutedID($lroms->[17], $lroms->[3]),                                                                   #17
		&getSessionID($lroms->[13]),                                                                               #18
		&convert_side($lroms->[4]),                                                                                #19
		&checkPrice($lroms->[7], $lroms->[8]),                                                                     #20
		$lroms->[6],                                                                                               #21 quantity
		$CATminQty,                                                                                                #22 &&get_minqty
		&convert_type($lroms->[8]),                                                                                #23
		&checkTif($lroms->[9], $lroms->[52]),                                                                      #24
		$CATtradingSession,                                                                                        #25
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[7],$na),                                         #26 &&handlingInstruction
		&checkReject($lroms->[17]),                                                                                #27
		&getOriginCode($lroms->[10], $lroms->[13]),	                                                        		#28
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
	my $na = shift;
	my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                      #1
		$CATerrorROEID,                                                                                      #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                         #3
		$CATtype[9],                                                                                         #4
		$CATReporterIMID,                                                                                    #5
		&getOriginalTime($lroms->[17], $lroms->[52]),                                                        #6 orderKeyDAte
		$lroms->[17],                                                                                        #7 orderID
		&getSymbol($lroms->[55], $lroms->[5], $lroms->[30], $lroms->[31], $lroms->[32], $lroms->[62]),       #8 OptionID
		$CATpriorOrderKeyDate,                                                                               #9
		$CATpriorOrderID,                                                                                    #10
		$CAToriginatingIMID,                                                                                 #11
		&getModifyTime($lroms->[59], $lroms->[52]),                                                          #12 eventTimestamp
		$CATmanualOrderKeyDate,                                                                              #13
		$CATmanualOrderID,                                                                                   #14
		$CATmanualFlag,                                                                                      #15 ???
		$CATelectronicDupFlag,                                                                               #16
		$CATelectronicTimestamp,                                                                             #17
		&get_receiver_for_mod($lroms->[12],$lroms->[10]),		#$CATreceiverIMID,                                       #18
		&get_sender_imid_for_clrid($lroms->[12],$lroms->[22]),                                                            #19
		&get_sender_type_for_mod($lroms->[12],$lroms->[10]),		#$CATsenderType[2],                                      #20 &&get_sender_type, 
		&get_routed_id_for_modify($lroms->[12],$lroms->[3], $lroms->[28], $lroms->[22]),                                   #21
		$CATinitiator,                                                                                       #22
		&convert_side($lroms->[4]),                                                                          #23
		&checkPrice($lroms->[7], $lroms->[8]),                                                               #24
		$lroms->[6],                                                                                         #25 quantity
		$CATminQty,,                                                                                         #26 &&get_minqty
		$lroms->[49],                                                                                        #27 leaveqty
		&convert_type($lroms->[8]),                                                                          #28 order type
		&checkTif($lroms->[9], $lroms->[52]),                                                                #29
		$CATtradingSession,                                                                                  #30
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[9],$na),                                   #31  &&handlingInstruction
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
	
	my $output2 = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		$CATactionType,                                                                                            #1
		$CATerrorROEID,                                                                                            #2
		&create_fore_id($lroms->[17], $lroms->[52]),                                                               #3
		$CATtype[7],                                                                                               #4
		$CATReporterIMID,                                                                                          #5
		&getOriginalTime($lroms->[17], $lroms->[52]),                                                              #6
		$lroms->[17],                                                                                              #7
		&getSymbol($lroms->[55],$lroms->[5], $lroms->[30], $lroms->[31], $lroms->[32], $lroms->[62]),              #8
		$CAToriginatingIMID,                                                                                       #9
		&getModifyTime($lroms->[59], $lroms->[52]),                                                                #10
		$CATmanualFlag,                                                                                            #11 ??
		$CATelectronicDupFlag,                                                                                     #12
		$CATelectronicTimestamp,                                                                                   #13
		&get_sender_imid_for_dest($lroms->[13], $lroms->[12]),                                                     #14 senderIMID
		&get_imid_for_dest($lroms->[13]),                                                                          #15
		&get_dest_type($lroms->[13]),                                                                              #16
		$lroms->[15],                                                                   							#17
		&getSessionID($lroms->[13]),                                                                               #18
		&convert_side($lroms->[4]),                                                                                #19
		&checkPrice($lroms->[7], $lroms->[8]),                                                                     #20
		$lroms->[6],                                                                                               #21 quantity
		$CATminQty,                                                                                                #22 &&get_minqty
		&convert_type($lroms->[8]),                                                                                #23
		&checkTif($lroms->[9], $lroms->[52]),                                                                      #24
		$CATtradingSession,                                                                                        #25
		&setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[7],$na),                                         #26 &&handlingInstruction
		&checkReject($lroms->[17]),                                                                                #27
		&getOriginCode($lroms->[10], $lroms->[13]),	                                                        		#28
		$CATaffiliateFlag[1],                                                                                      #29 &&get_affiliation
		$CATmultiLegInd,                                                                                           #30
		&getOpenClose($lroms->[38]),                                                                               #31
		$CATretiredFieldPosition,                                                                                  #32
		$CATretiredFieldPosition,                                                                                  #33
		$CATpairedOrderID,                                                                                         #34
		$CATnetPrice                                                                                               #35 &&get_net_price
	);
	print $lf $output2;
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
		$lroms->[17],                                                                                   #7 orderID
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

#
#
#

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
		print "Could not find sending time for $myLastID \n";
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
# 	sprintf("%s_DART_%04d%02d%02d_OrderEvents_%06d.csv", $who, $year + 1900, $mon + 1, $mday, $file_sequence);
# }

sub create_file {
	my $who = shift;
	my $input_day = shift;
	if(defined $input_day){
		sprintf("%s_DART_%d_BOLTOption_OrderEvents_%06d.csv", $who, $input_day, $file_sequence);
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
		sprintf("%s_DART_%04d%02d%02d_BOLTOption_OrderEvents_%06d.csv", $who, $year + 1900, $mon + 1, $mday, $file_sequence);
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


sub get_sender_imid_for_dest {#13,#12
	my $dest = shift;
	my $acc = shift;
	my $imid = $senderimids{$dest};
	if(defined $imid) {
		if($acc eq "A4SA1209" || ($acc eq "A4SA" && $exchid{$dest} eq "CBOE")  || $acc eq "888895") {
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
	my $cap = shift;
	if($acc eq "A4SA" and $cap eq "X") {
		""
	} else {
		$CATreceiverIMID;
	}
}

sub get_sender_type_for_mod {
	my $acc = shift;
	my $cap = shift;
	if($acc eq "A4SA" and $cap eq "X") {
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
    if($type eq "1" and $event eq "MOOA"){
    	"DIR|NH"
	}elsif($event eq "MOOA" or ($naind eq "A" and $event eq "MOOM")){
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
    }elsif($naind eq "A" and $event eq "MOOR"){
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
    

#### modification note
# 1/7/2022: 
# added "" to sub get_sender_imid_for_clrid for acct A4SA
# add sub get_receiver_for_mod 
# add sub get_sender_type_for_mod
# 2/4/2022:
# add sub getAcctHolerType
# in sub create_new_order: use &getAcctHolerType($lroms->[12]), for column 25.
# add MOOA to get DIR instruction in sub setHandlingInstructions
# 2/7/2022
# change deptType for MOOA from "O" to "A"
# add DIR to MOOM event.
# 2/11/2022: not in prod yet
# trunk the decimal digits to 8 for price in sub checkPrice.
# 20220214
# added non_reports hashmap and lines to filter out future destinations from be processed for CAT.
# 4/4/2022 add "-" between the romtag and the sequence number in sub create_fore_id
# 4/8/2022 new exchange 188 and 190 in the hashmaps
# 4/12/2022 changed the desitnation from EDGX to EDGXOP for 188.
# 5/3/2022 add "EDDY" and "WFS" to clientimid map
# 5/3/2022 updated sub get_routed_id_for_modify and sub get_sender_imid_for_clrid to use the value in field 22
# 5/15/2022 add capacity parameter to the subs get_receiver_for_mod and get_sender_type_for_mod to address B capacity for A4SA clearing account.
# 6/6/2022: added 'WFSAVGPX' => '126292:WCHV' to clientimid hash map and modified subs 'get_routed_id_for_modify' and 'get_sender_imid_for_clrid'
# 20220608: add "DIR|NH" instruction and change "DIR|RAR" to "DIR" in sub setHandlingInstructions
# 20220712: change the side value of SL, SS and SX to S in sub convert_side.
# 20230127: remove a , on line 25