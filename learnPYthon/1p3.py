#. Design an air traffic control unit. The runway should be either free or busy, Each plane should request for landing if the plane is given permission for landing the runway should be changed to busy and no plane should be given access to that runway.

#it means in airport u have 2 roads, 1 road for arrival , road 2 is dep, i want to give signal to the flight ... busy_runways means already approved to another flight so dont give signal to any flight
#free_runways means if any flight coming give singal in this road like that i want to give signal.

def request_landing(free_runways, busy_runways, plane):
  if not free_runways:
    return None

  runway = free_runways.pop()
  busy_runways.append(runway)

  print("The runway has been given:", runway)

  return runway

def land(busy_runways, free_runways, plane):
  if not busy_runways:
    return

  runway = busy_runways.pop()
  free_runways.append(runway)

  print("Landed:", "YES")

# Initialize the lists of free and busy runways
free_runways = [3, 4, 5]
busy_runways = [1, 2]

# Request a landing for two planes
plane1_runway = request_landing(free_runways, busy_runways, "Plane 1")
plane2_runway = request_landing(free_runways, busy_runways, "Plane 2")

# Land the first plane
land(busy_runways, free_runways, "Plane 1")

# Request a landing for a third plane
plane3_runway = request_landing(free_runways, busy_runways, "Plane 3")

# Land the second plane
land(busy_runways, free_runways, "Plane 2")

