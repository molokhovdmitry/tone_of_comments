const form = document.querySelector('form');
const predictButton = document.querySelector('form button');

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  
  // Disable the Predict button and show loading animation
  predictButton.disabled = true;
  predictButton.innerHTML = 'Loading...';

  const videoId = form.elements['video_id'].value;

  const response = await fetch(`/predict?video_id=${videoId}`, {
    method: 'POST',
    headers: {
      'accept': 'application/json'
    },
  });
const data = await response.json();

if ('error' in data) {
  alert('Invalid ID. Please enter a valid YouTube video ID.');
} else {
  // Create data rows and count the number of emotions
  const emotionCounts = {};
  Object.keys(data.Response).forEach(key => {
    const rowData = data.Response[key];
    Object.keys(rowData.emotions).forEach(emotion => {
      if (emotionCounts[emotion] === undefined) {
        emotionCounts[emotion] = rowData.emotions[emotion];
      } else {
        emotionCounts[emotion] += rowData.emotions[emotion];
      }
    });
  });

  // Sort the emotionCounts object by values
  const sortedEmotionCounts = Object.entries(emotionCounts)
    .sort(([, countA], [, countB]) => countB - countA)
    .reduce((sortedObj, [key, value]) => ({ ...sortedObj, [key]: value }), {});

  // Plot the emotion counts using Plotly
  const emotions = [{
    x: Object.keys(sortedEmotionCounts),
    y: Object.values(sortedEmotionCounts),
    type: 'bar',
    marker: {
      color: 'orange'
    }
  }];

  const layout = {
    plot_bgcolor: '#111',
    paper_bgcolor: '#111',
    font: {
      color: '#fff'
    }
  };

  Plotly.newPlot('plot', emotions, layout);
}

// Re-enable the Predict button
predictButton.disabled = false;
predictButton.innerHTML = 'Predict';
});
