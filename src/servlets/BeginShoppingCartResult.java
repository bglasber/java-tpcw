package servlets;

import java.util.Map;

import com.dynamic.mastering.primary_key;

import dm.DMConnId;

class BeginShoppingCartResult {
  public Map<primary_key, DMConnId> writeLocations;
  public int shoppingId;

  public BeginShoppingCartResult(Map<primary_key, DMConnId> writeLocations,
                                 int shoppingId) {
    this.writeLocations = writeLocations;
    this.shoppingId = shoppingId;
  }
}
