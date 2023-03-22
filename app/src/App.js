import "./App.css";
import Board from "./Board";
import React, { useState } from "react";
import Setting from "./Setting";

function App() {
  // Default selected location as San Francisco
  const [location, setLocation] = useState("San Francisco");
  const [numPanels, setNumPanels] = useState(1);
  const [efficiency, setEfficiency] = useState(200);

  return (
    <div className="App">
      <Setting
        selectLocation={setLocation}
        selectNumPanels={setNumPanels}
        selectEfficiency={setEfficiency}
        numPanels={numPanels}
        efficiency={efficiency}
      />
      <Board
        location={location}
        numPanels={numPanels}
        efficiency={efficiency}
      />
    </div>
  );
}

export default App;
