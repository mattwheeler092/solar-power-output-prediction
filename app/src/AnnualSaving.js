import React, { useState, useEffect } from "react";
import { FaSolarPanel } from "react-icons/fa";
import predictions from "./predictions.json";

export default function AnnualSaving(props) {
  const [saving, setSaving] = useState(512);

  // Function for computing annual savings
  function computeYearlyCostSavings(irradianceTotal, numPanels, maxPanelPower) {
    const ENERGY_COST = 0.0003; // $0.30 per kWh
    const MONTHLY_HOUSEHOLD_ENERGY_USAGE = 600000; // 600 KWH
    const totalPanelEnergy =
      numPanels * maxPanelPower * (irradianceTotal / 1000);
    const proportionEnergyBill = Math.min(
      1,
      totalPanelEnergy / (12 * MONTHLY_HOUSEHOLD_ENERGY_USAGE)
    );
    const savings =
      Math.min(totalPanelEnergy, 12 * MONTHLY_HOUSEHOLD_ENERGY_USAGE) *
      ENERGY_COST;
    return [savings.toFixed(2), (100 * proportionEnergyBill).toFixed(1)];
  }

  return (
    <>
      <div className="solar">
        <FaSolarPanel size={"3em"} />{" "}
        <div>
          <p> Predicted Annual Saving </p>{" "}
          <h1>
            {" "}
            ${" "}
            {
              computeYearlyCostSavings(
                predictions[props.location],
                props.numPanels,
                props.efficiency
              )[0]
            }{" "}
          </h1>{" "}
          <div className="pct_saving">
            <h3>
              {" "}
              {
                computeYearlyCostSavings(
                  predictions[props.location],
                  props.numPanels,
                  props.efficiency
                )[1]
              }
              %{" "}
            </h3>{" "}
            <text> of annual electricity bill </text>{" "}
          </div>{" "}
        </div>{" "}
      </div>{" "}
      <div id="asterisk">
        <span>
          Assumed a yearly household energy consumption of 600 kWh and an
          average energy cost of $0 .30 per kWh{" "}
        </span>{" "}
      </div>{" "}
    </>
  );
}
