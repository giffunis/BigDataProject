
package parte1;

import java.util.ArrayList;
import java.util.List;

public class MeterBootstrapping {

    public static List<SynteticData> generateSyntheticReadings(List<OriginalData> originalData, int factorF, int factorC) {
        List<SynteticData> synteticData = new ArrayList<SynteticData>();
    	
        for (OriginalData original : originalData) {	
        		List<String> measures = new ArrayList<String>();
        		
        		for (int c = 1; c <= factorC; c++) {
					measures.add(original.getMeasure());
				}
        		
        		for (int f = 1; f <= factorF; f++) {	
        			synteticData.add(new SynteticData(String.format("%d%s", f, original.getSensor()), original.getDatetime(), measures));
        		}	
		}
        
        return synteticData;
    }
}
