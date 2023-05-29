/*
MIT License

Copyright (c) 2023 molokhovdmitry

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

const form = document.querySelector('form');
const predictButton = document.querySelector('form button');

function plotData(data) {
  if ('error' in data) {
    alert('Invalid ID. Please enter a valid YouTube video ID.');
  } else {
    // Count the number of emotions
    const emotionCounts = {
      anger: 0,
      anticipation: 0,
      disgust: 0,
      fear: 0,
      joy: 0,
      love: 0,
      optimism: 0,
      hopeless: 0,
      sadness: 0,
      surprise: 0,
      trust: 0
    };
    Object.keys(data.Response.comments).forEach(key => {
      const rowData = data.Response.comments[key];
      Object.keys(rowData).forEach(key => {
        if (emotionCounts.hasOwnProperty(key)){
          emotionCounts[key] += rowData[key]
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
          color: '#fff',
          size: 16
        }
    };

    Plotly.newPlot('plot', emotions, layout);
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  
  // Disable the Predict button and show loading animation
  predictButton.disabled = true;
  predictButton.innerHTML = 'Loading...';

  const videoId = form.elements['video_id'].value;
  processComments(videoId);


  async function processComments(videoId) {
    try {
      const url = `/predict?video_id=${videoId}`;
      const options = {
        method: 'POST',
        headers: {'accept': 'application/json'}
      };
      const response = await fetch(url, options);
      const reader = response.body.getReader();
      const decoder = new TextDecoder('utf-8');
      let jsonString = ''
      while (true) {
        // Read responses in chunks and decode as UTF-8
        const { done, value } = await reader.read();
        const string = decoder.decode(value);

        // Accumulate JSON from chunks, plot when complete
        const endOfJson = string.endsWith('}]}}');
        if (string.trim() !== '') {
            if (jsonString == '' || endOfJson)  {
                // Append first or last chunk
                jsonString += string;
                if (endOfJson) {
                    // Parse JSON and plot data if last chunk
                    const json = JSON.parse(jsonString);
                    plotData(json);
                    jsonString = '';
                }
            } else {
                // Append middle chunk
                jsonString += string;
            }
        }
        if (done) {
          // Re-enable the Predict button
          predictButton.disabled = false;
          predictButton.innerHTML = 'Predict';
          break;
        }
      }
    } catch (e) {
      if (e instanceof TypeError) {
        console.log(e);
      } else {
        console.log(`Error in async iterator: ${e}.`);
      }
    }
  }
});

