// The river stations used in the labs, with their warning levels in metres.
// Sample values for training only. These are not official JPS station levels.
export const STATIONS = [
  { id: 'KLT01', river: 'Sungai Kelantan', location: 'Kuala Krai', state: 'Kelantan', normal: 18.0, alert: 21.0, warning: 23.0, danger: 25.0 },
  { id: 'PHG01', river: 'Sungai Pahang', location: 'Temerloh', state: 'Pahang', normal: 30.0, alert: 32.0, warning: 33.5, danger: 35.0 },
  { id: 'TRG01', river: 'Sungai Terengganu', location: 'Kuala Berang', state: 'Terengganu', normal: 10.0, alert: 12.0, warning: 13.0, danger: 14.0 },
  { id: 'SGR01', river: 'Sungai Selangor', location: 'Rasa', state: 'Selangor', normal: 25.0, alert: 26.5, warning: 27.5, danger: 28.5 },
  { id: 'KDH01', river: 'Sungai Muda', location: 'Kuala Ketil', state: 'Kedah', normal: 6.0, alert: 7.5, warning: 8.5, danger: 9.5 },
  { id: 'JHR01', river: 'Sungai Johor', location: 'Kota Tinggi', state: 'Johor', normal: 3.0, alert: 4.0, warning: 4.5, danger: 5.0 },
];

export function findStation(id) {
  return STATIONS.find((station) => station.id === id);
}

// The JPS stages: NORMAL → WASPADA (alert) → AMARAN (warning) → BAHAYA (danger)
export function levelStatus(station, level) {
  if (level >= station.danger) return 'BAHAYA';
  if (level >= station.warning) return 'AMARAN';
  if (level >= station.alert) return 'WASPADA';
  return 'NORMAL';
}
