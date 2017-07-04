package servlets;

import java.util.List;

import com.dynamic.mastering.primary_key;

import dm.DMConnId;

class DMKeyToItemMeaning {

  public String tableName;
  public List<String> columns;
  public List<String> identifiers;

  public DMKeyToItemMeaning(String tableName, List<String> columns,
                            List<String> identifiers) {
    this.tableName = tableName;
    this.columns = columns;
    this.identifiers = identifiers;
  }
}


