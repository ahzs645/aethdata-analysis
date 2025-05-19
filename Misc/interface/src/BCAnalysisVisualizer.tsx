import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ScatterChart, Scatter, ResponsiveContainer } from 'recharts';

interface DataPoint {
  date: string;
  timestamp: number;
  month: number;
  season: string;
  redBCc: number;
  ecFTIR: number;
  fabs: number;
  mac: number;
}

const BCAnalysisVisualizer = () => {
  const [data, setData] = useState<DataPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [view, setView] = useState('scatter'); // 'scatter', 'timeseries', 'mac'
  const [seasonFilter, setSeasonFilter] = useState('all');

  useEffect(() => {
    const generateSampleData = (): DataPoint[] => {
      // This is a placeholder for the actual data that would be loaded
      // In a real implementation, we would fetch data from the analysis
      
      // Create sample data representing the kind of output we'd expect from the analysis
      const startDate = new Date('2022-01-01');
      const sampleSize = 103; // The 103 days mentioned in the request
      
      const seasons = ['Dry Season', 'Belg Rainy Season', 'Kiremt Rainy Season'];
      const seasonMonths = {
        'Dry Season': [10, 11, 12, 1, 2],
        'Belg Rainy Season': [3, 4, 5],
        'Kiremt Rainy Season': [6, 7, 8, 9]
      };
      
      const sampleData: DataPoint[] = [];
      
      for (let i = 0; i < sampleSize; i++) {
        const date = new Date(startDate);
        date.setDate(startDate.getDate() + i * 3); // Space out over time
        
        const month = date.getMonth() + 1; // JavaScript months are 0-based
        
        // Determine season
        let season = '';
        for (const [s, months] of Object.entries(seasonMonths)) {
          if (months.includes(month)) {
            season = s;
            break;
          }
        }
        
        // Base values with some relationship between them
        const baseValue = 2 + Math.random() * 8; // Range from 2-10
        
        // Create some seasonal variation
        let seasonalFactor = 1.0;
        if (season === 'Dry Season') seasonalFactor = 1.2;
        if (season === 'Belg Rainy Season') seasonalFactor = 0.8;
        
        // Generate correlated values with some noise
        const redBCc = baseValue * seasonalFactor * (1 + (Math.random() - 0.5) * 0.3);
        const ecFTIR = baseValue * 0.9 * (1 + (Math.random() - 0.5) * 0.4); // Slight bias lower
        const fabs = baseValue * 5 * (1 + (Math.random() - 0.5) * 0.5); // Different scale
        
        // Calculate MAC value (typical values around 7-10 m²/g with variation)
        const mac = fabs / ecFTIR;
        
        sampleData.push({
          date: date.toISOString().split('T')[0],
          timestamp: date.getTime(),
          month,
          season,
          redBCc,
          ecFTIR,
          fabs,
          mac
        });
      }
      
      return sampleData;
    };
    
    try {
      const sampleData = generateSampleData();
      setData(sampleData);
      setLoading(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "An unknown error occurred");
      setLoading(false);
    }
  }, []);

  const filteredData = data.filter(item => 
    seasonFilter === 'all' || item.season === seasonFilter
  );

  if (loading) return <div className="text-center p-4">Loading data...</div>;
  if (error) return <div className="text-center p-4 text-red-600">{error}</div>;

  const renderScatterPlot = () => (
    <div className="space-y-4">
      <h3 className="text-xl font-bold text-center">Red BCc vs FTIR EC</h3>
      <ResponsiveContainer width="100%" height={400}>
        <ScatterChart
          margin={{ top: 20, right: 20, bottom: 20, left: 20 }}
        >
          <CartesianGrid />
          <XAxis 
            type="number" 
            dataKey="redBCc" 
            name="Red BCc" 
            label={{ value: 'Aethalometer Red BCc (μg/m³)', position: 'bottom', offset: 0 }} 
            domain={[0, 'dataMax + 1']}
          />
          <YAxis 
            type="number" 
            dataKey="ecFTIR" 
            name="FTIR EC" 
            label={{ value: 'FTIR EC (μg/m³)', angle: -90, position: 'left' }} 
            domain={[0, 'dataMax + 1']}
          />
          <Tooltip 
            formatter={(value: number) => [value.toFixed(2), '']}
            labelFormatter={(_, payload) => {
              if (payload && payload.length > 0) {
                return `Date: ${payload[0].payload.date}
Red BCc: ${payload[0].payload.redBCc.toFixed(2)} μg/m³
FTIR EC: ${payload[0].payload.ecFTIR.toFixed(2)} μg/m³
Season: ${payload[0].payload.season}`;
              }
              return '';
            }}
          />
          <Legend />
          <Scatter 
            name={seasonFilter === 'all' ? 'All Seasons' : seasonFilter} 
            data={filteredData} 
            fill="#8884d8" 
          />
          {/* Reference 1:1 line */}
          <Line 
            name="1:1 Line" 
            data={[{x: 0, y: 0}, {x: 15, y: 15}]} 
            dataKey="y" 
            xAxisId="redBCc" 
            stroke="#ff7300" 
            dot={false} 
            activeDot={false}
            strokeDasharray="5 5"
          />
        </ScatterChart>
      </ResponsiveContainer>

      <h3 className="text-xl font-bold text-center mt-8">Red BCc vs HIPS Fabs</h3>
      <ResponsiveContainer width="100%" height={400}>
        <ScatterChart
          margin={{ top: 20, right: 20, bottom: 20, left: 20 }}
        >
          <CartesianGrid />
          <XAxis 
            type="number" 
            dataKey="redBCc" 
            name="Red BCc" 
            label={{ value: 'Aethalometer Red BCc (μg/m³)', position: 'bottom', offset: 0 }} 
            domain={[0, 'dataMax + 1']}
          />
          <YAxis 
            type="number" 
            dataKey="fabs" 
            name="HIPS Fabs" 
            label={{ value: 'HIPS Fabs', angle: -90, position: 'left' }} 
            domain={[0, 'dataMax + 5']}
          />
          <Tooltip 
            formatter={(value: number) => [value.toFixed(2), '']}
            labelFormatter={(_, payload) => {
              if (payload && payload.length > 0) {
                return `Date: ${payload[0].payload.date}
Red BCc: ${payload[0].payload.redBCc.toFixed(2)} μg/m³
HIPS Fabs: ${payload[0].payload.fabs.toFixed(2)}
Season: ${payload[0].payload.season}`;
              }
              return '';
            }}
          />
          <Legend />
          <Scatter 
            name={seasonFilter === 'all' ? 'All Seasons' : seasonFilter} 
            data={filteredData} 
            fill="#82ca9d" 
          />
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );

  const renderTimeSeriesPlot = () => {
    // Sort data by date for time series
    const sortedData = [...filteredData].sort((a, b) => a.timestamp - b.timestamp);
    
    return (
      <div className="space-y-4">
        <h3 className="text-xl font-bold text-center">Time Series of Measurements</h3>
        <ResponsiveContainer width="100%" height={500}>
          <LineChart
            data={sortedData}
            margin={{ top: 20, right: 30, left: 20, bottom: 60 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis 
              dataKey="date" 
              angle={-45} 
              textAnchor="end" 
              height={80} 
              label={{ value: 'Date', position: 'bottom', offset: 20 }}
            />
            <YAxis 
              yAxisId="left"
              label={{ value: 'Concentration (μg/m³)', angle: -90, position: 'insideLeft' }} 
            />
            <YAxis 
              yAxisId="right" 
              orientation="right" 
              label={{ value: 'HIPS Fabs', angle: 90, position: 'insideRight' }} 
            />
            <Tooltip 
              formatter={(value: number, name: string) => [value.toFixed(2), name]}
              labelFormatter={(label) => `Date: ${label}`}
            />
            <Legend />
            <Line 
              yAxisId="left"
              type="monotone" 
              dataKey="redBCc" 
              name="Red BCc (μg/m³)" 
              stroke="#8884d8" 
              dot={{ r: 4 }} 
              activeDot={{ r: 8 }} 
            />
            <Line 
              yAxisId="left"
              type="monotone" 
              dataKey="ecFTIR" 
              name="FTIR EC (μg/m³)" 
              stroke="#82ca9d" 
              dot={{ r: 4 }} 
              activeDot={{ r: 8 }} 
            />
            <Line 
              yAxisId="right"
              type="monotone" 
              dataKey="fabs" 
              name="HIPS Fabs" 
              stroke="#ff7300" 
              dot={{ r: 4 }} 
              activeDot={{ r: 8 }} 
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    );
  };

  const renderMACAnalysis = () => {
    // Prepare MAC data
    const macData = filteredData.map(item => ({
      date: item.date,
      mac: item.mac,
      season: item.season
    }));
    
    // Calculate stats
    const macValues = macData.map(item => item.mac);
    const avgMAC = macValues.reduce((sum, val) => sum + val, 0) / macValues.length;
    
    // Seasonal stats
    const seasonalMACs: Record<string, { avg: string; count: number }> = {};
    seasons.forEach(season => {
      const seasonData = macData.filter(item => item.season === season).map(item => item.mac);
      if (seasonData.length > 0) {
        const avg = seasonData.reduce((sum, val) => sum + val, 0) / seasonData.length;
        seasonalMACs[season] = {
          avg: avg.toFixed(2),
          count: seasonData.length
        };
      }
    });
    
    // Sort data by date for time series
    const sortedData = [...macData].sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
    
    return (
      <div className="space-y-4">
        <h3 className="text-xl font-bold text-center">Mass Absorption Cross-section (MAC) Analysis</h3>
        
        <div className="bg-gray-100 p-4 rounded-lg">
          <p className="font-bold">Overall MAC (Red Channel): {avgMAC.toFixed(2)} m²/g (n={macValues.length})</p>
          
          <p className="font-bold mt-2">Seasonal MAC Values:</p>
          <ul className="list-disc pl-5">
            {Object.entries(seasonalMACs).map(([season, stats]) => (
              <li key={season}>{season}: {stats.avg} m²/g (n={stats.count})</li>
            ))}
          </ul>
        </div>
        
        <ResponsiveContainer width="100%" height={400}>
          <LineChart
            data={sortedData}
            margin={{ top: 20, right: 30, left: 20, bottom: 60 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis 
              dataKey="date" 
              angle={-45} 
              textAnchor="end" 
              height={80} 
              label={{ value: 'Date', position: 'bottom', offset: 20 }}
            />
            <YAxis 
              label={{ value: 'MAC (m²/g)', angle: -90, position: 'insideLeft' }} 
            />
            <Tooltip 
              formatter={(value: number) => [value.toFixed(2), 'MAC (m²/g)']}
              labelFormatter={(label) => `Date: ${label}`}
            />
            <Legend />
            <Line 
              type="monotone" 
              dataKey="mac" 
              name="Red MAC (m²/g)" 
              stroke="#e74c3c" 
              dot={{ r: 4 }} 
              activeDot={{ r: 8 }} 
            />
            <Line 
              type="monotone" 
              dataKey="mac" 
              name="Average MAC" 
              stroke="#3498db" 
              dot={false} 
              strokeDasharray="5 5"
              strokeWidth={2}
              legendType="none"
              isAnimationActive={false}
              data={[
                { date: sortedData[0]?.date, mac: avgMAC },
                { date: sortedData[sortedData.length-1]?.date, mac: avgMAC }
              ]}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    );
  };

  // Define available Ethiopian seasons
  const seasons = ['Dry Season', 'Belg Rainy Season', 'Kiremt Rainy Season'];

  return (
    <div className="p-4 max-w-6xl mx-auto">
      <h1 className="text-2xl font-bold text-center mb-4">Black Carbon Cross-Method Analysis</h1>
      <p className="text-center mb-6">
        Analysis of excellent quality data (≤10 min missing) for the 103 days with overlapping measurements 
        between Aethalometer Red BCc, FTIR EC, and HIPS Fabs measurements.
      </p>
      
      <div className="mb-6 flex flex-wrap justify-center gap-4">
        <div>
          <label className="mr-2 font-medium">View:</label>
          <select 
            value={view} 
            onChange={(e) => setView(e.target.value)}
            className="border rounded p-2"
          >
            <option value="scatter">Scatter Plots</option>
            <option value="timeseries">Time Series</option>
            <option value="mac">MAC Analysis</option>
          </select>
        </div>
        
        <div>
          <label className="mr-2 font-medium">Season Filter:</label>
          <select 
            value={seasonFilter} 
            onChange={(e) => setSeasonFilter(e.target.value)}
            className="border rounded p-2"
          >
            <option value="all">All Seasons</option>
            {seasons.map(season => (
              <option key={season} value={season}>{season}</option>
            ))}
          </select>
        </div>
      </div>
      
      {view === 'scatter' && renderScatterPlot()}
      {view === 'timeseries' && renderTimeSeriesPlot()}
      {view === 'mac' && renderMACAnalysis()}
      
      <div className="mt-8 bg-gray-100 p-4 rounded-lg">
        <h3 className="font-bold">Data Summary:</h3>
        <ul className="list-disc ml-5">
          <li>Total Excellent Quality Days: {data.length}</li>
          <li>Days in current selection: {filteredData.length}</li>
          <li>Seasonal Distribution:
            <ul className="list-circle ml-5">
              {seasons.map(season => {
                const count = data.filter(item => item.season === season).length;
                return <li key={season}>{season}: {count} days ({(count/data.length*100).toFixed(1)}%)</li>;
              })}
            </ul>
          </li>
        </ul>
      </div>
    </div>
  );
};

export default BCAnalysisVisualizer; 