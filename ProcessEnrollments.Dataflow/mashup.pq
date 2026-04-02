[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "d2a440f4-6d00-4354-9f77-a2fcae66e547"]}[Data]{[lakehouseId = "e513c730-c3e9-4ee4-9417-e402e73abdf2"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "courses_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared courses = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d2a440f4-6d00-4354-9f77-a2fcae66e547"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "54e9c533-df37-4481-949c-e08554246a41"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "courses", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "enrollments_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
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
  #"Navigation 2" = #"Navigation 1"{[Id = "students", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "terms_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared terms = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d2a440f4-6d00-4354-9f77-a2fcae66e547"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "54e9c533-df37-4481-949c-e08554246a41"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "terms", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared enrollments_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "353b3277-e048-434b-8858-abad8e834b75"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "f557bcf8-c013-4c9c-a592-46857301eadb"]}[Data],
  TableNavigation = Navigation_2{[Id = "enrollments", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared terms_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "353b3277-e048-434b-8858-abad8e834b75"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "f557bcf8-c013-4c9c-a592-46857301eadb"]}[Data],
  TableNavigation = Navigation_2{[Id = "terms", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared courses_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "353b3277-e048-434b-8858-abad8e834b75"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "f557bcf8-c013-4c9c-a592-46857301eadb"]}[Data],
  TableNavigation = Navigation_2{[Id = "courses", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared students_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "353b3277-e048-434b-8858-abad8e834b75"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "f557bcf8-c013-4c9c-a592-46857301eadb"]}[Data],
  TableNavigation = Navigation_2{[Id = "students", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
