import React, { useState } from "react";
import { Slider, Tooltip, Typography } from "@mui/material";
import { FaSolarPanel } from "react-icons/fa";
import { MdOutlineElectricalServices } from "react-icons/md";

export default function SolarPanelSlider(props) {
  const [isOpen, setIsOpen] = useState(false);

  function handleIconClick() {
    setIsOpen(!isOpen);
  }

  return (
    <div>
      <div className="iconButton" onClick={handleIconClick}>
        {" "}
        {props.icon === "panel" ? (
          <Tooltip title="Number of panels" arrow>
            <span>
              <FaSolarPanel size={30} />{" "}
            </span>
          </Tooltip>
        ) : (
          <Tooltip
            title={
              <Typography variant="span">
                Power Efficiency W/M<sup>2</sup>
              </Typography>
            }
            arrow
          >
            <span>
              <MdOutlineElectricalServices size={30} />{" "}
            </span>
          </Tooltip>
        )}{" "}
        <Slider
          min={props.min}
          max={props.max}
          defaultValue={props.defaultValue}
          valueLabelDisplay="auto"
          onChange={props.onChange}
        />{" "}
      </div>{" "}
    </div>
  );
}
