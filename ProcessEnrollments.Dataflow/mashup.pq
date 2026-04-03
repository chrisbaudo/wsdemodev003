[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "d2a440f4-6d00-4354-9f77-a2fcae66e547"]}[Data]{[lakehouseId = "e513c730-c3e9-4ee4-9417-e402e73abdf2"]}[Data];
[BindToDefaultDestination = true]
shared courses = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d2a440f4-6d00-4354-9f77-a2fcae66e547"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "54e9c533-df37-4481-949c-e08554246a41"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "courses", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared enrollments = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d2a440f4-6d00-4354-9f77-a2fcae66e547"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "54e9c533-df37-4481-949c-e08554246a41"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "enrollments", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "students_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared students = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d2a440f4-6d00-4354-9f77-a2fcae66e547"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "54e9c533-df37-4481-949c-e08554246a41"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "students", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"date_of_birth", type date}}),
  #"Added custom" = Table.AddColumn(#"Changed column type", "birth_year", each Date.Year([date_of_birth])),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "birth_month", each Date.Month([date_of_birth])),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 1", {{"birth_year", Int64.Type}, {"birth_month", Int64.Type}})
in
  #"Changed column type 1";
[BindToDefaultDestination = true]
shared terms = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d2a440f4-6d00-4354-9f77-a2fcae66e547"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "54e9c533-df37-4481-949c-e08554246a41"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "terms", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared students_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d2a440f4-6d00-4354-9f77-a2fcae66e547"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e513c730-c3e9-4ee4-9417-e402e73abdf2"]}[Data],
  TableNavigation = Navigation_2{[Id = "students", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
