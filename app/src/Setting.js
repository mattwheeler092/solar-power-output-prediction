import React, { useState } from "react";
import "./Setting.css";
import SelectSearch from "react-select-search";
import location_image from "./location_image.json";
import Slider from "./Slider";

export default function Setting(props) {
  const onChange = (location) => {
    props.selectLocation(location);
  };

  const options = Object.entries(location_image).map(([name, value]) => {
    return {
      name: name,
      value: name,
    };
  });

  return (
    <form>
      <div className="search_container">
        <SelectSearch
          className="select-search"
          options={options}
          search
          placeholder="Location"
          value=""
          onChange={(value) => onChange(value)}
        />
        <div className="parameter_button">
          <Slider
            icon="panel"
            min={1}
            defaultValue={props.numPanels}
            max={20}
            onChange={(e) => props.selectNumPanels(e.target.value)}
          />
          <Slider
            icon="power"
            min={50}
            defaultValue={props.efficiency}
            max={400}
            onChange={(e) => props.selectEfficiency(e.target.value)}
          />
        </div>
      </div>
    </form>
  );
}
