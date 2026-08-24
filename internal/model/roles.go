package model

// CanVote reports whether a role may participate in a new election term.
func CanVote(role Role) bool {
	switch role {
	case RoleFollower, RoleCandidate, RoleResigned:
		return true
	default:
		return false
	}
}

// CanLead reports whether the role is allowed to serve requests.
func CanLead(role Role) bool {
	return role == RoleLeader
}

// Transition validates a role change and returns the next role.
func Transition(from, to Role) (Role, bool) {
	switch from {
	case RoleFollower:
		if to == RoleCandidate || to == RoleLeader || to == RoleResigned {
			return to, true
		}
	case RoleCandidate:
		if to == RoleLeader || to == RoleFollower || to == RoleResigned {
			return to, true
		}
	case RoleLeader:
		if to == RoleFollower || to == RoleResigned {
			return to, true
		}
	case RoleResigned:
		if to == RoleFollower || to == RoleCandidate {
			return to, true
		}
	}
	return from, false
}
