const functions = require('@google-cloud/functions-framework')
const axios = require('axios')
const Papa = require('papaparse')

const api = axios.create({ baseURL: 'https://api.icehockey.hu' })
api.defaults.headers.common['x-api-key'] =
  '7b4f4d1b466b5a3572990ae24452abf2a086e7ee'
api.defaults.headers.common['origin'] = 'https://www.jegkorongszovetseg.hu'

const getGameData = async (gameId) => {
  console.log(`getting game data ${gameId}`)
  const { data: stats } = await api.get(`/vbr/v1/gameStats?gameId=${gameId}`)
  const { data: events } = await api.get(`/vbr/v1/gameEvents?gameId=${gameId}`)
  return { stats, events }
}

const getCalendar = async (seasonId) => {
  console.log(`getting calendar ${seasonId}`)
  const { data: calendar } = await api.get(
    `/vbr/v1/publicCalendar?seasonId=${seasonId}`
  )
  return calendar
}

const isGoal = (event) => event.type === 'Gól'
const isScorer = (player) => (event) => event.playerId == player.id
const isAssist1 = (player) => (event) => event.assists1 == player.id
const isAssist2 = (player) => (event) => event.assists2 == player.id
const isENGoal = (event) => event.en
const isPSGoal = (event) => event.ps
const isPP1Goal = (event) => event.advantage === 'PP1'
const isPP2Goal = (event) => event.advantage === 'PP2'
const isGoalFor = (player) => (event) =>
  event.teamId == player.teamId &&
  event.type === 'Gól' &&
  (event.homeOnIce.split(',').includes(String(player.id)) ||
    event.awayOnIce.split(',').includes(String(player.id)))
const isGoalAgainst = (player) => (event) =>
  event.teamId != player.teamId &&
  event.type === 'Gól' &&
  (event.homeOnIce.split(',').includes(String(player.id)) ||
    event.awayOnIce.split(',').includes(String(player.id)))

const getGameJson = ({ stats, events }) => {
  const teamIds = Object.keys(stats.data.players)
  const players = teamIds
    .flatMap((teamId) => stats.data.players[teamId])
    .filter((player) => player.position !== 'GK')
    .map((player) => ({
      ...player,
      // convert strings to number
      pim: Number(player.pim || 0),
      plusMinus: Number(player.plusMinus || 0),
      // players don't have teamName...
      teamName: stats.data.goalies[player.teamId][0].teamName,
      // count extra data
      gp: 1,
      assists1: events.data.filter(isGoal).filter(isAssist1(player)).length,
      assists2: events.data.filter(isGoal).filter(isAssist2(player)).length,
      pp1Goals: events.data
        .filter(isGoal)
        .filter(isScorer(player))
        .filter(isPP1Goal).length,
      pp2Goals: events.data
        .filter(isGoal)
        .filter(isScorer(player))
        .filter(isPP2Goal).length,
      psGoals: events.data
        .filter(isGoal)
        .filter(isScorer(player))
        .filter(isPSGoal).length,
      enGoals: events.data
        .filter(isGoal)
        .filter(isScorer(player))
        .filter(isENGoal).length,
      pp1Assists1: events.data
        .filter(isGoal)
        .filter(isAssist1(player))
        .filter(isPP1Goal).length,
      pp1Assists2: events.data
        .filter(isGoal)
        .filter(isAssist2(player))
        .filter(isPP1Goal).length,
      pp2Assists1: events.data
        .filter(isGoal)
        .filter(isAssist1(player))
        .filter(isPP2Goal).length,
      pp2Assists2: events.data
        .filter(isGoal)
        .filter(isAssist2(player))
        .filter(isPP2Goal).length,
      enAssists1: events.data
        .filter(isGoal)
        .filter(isAssist1(player))
        .filter(isENGoal).length,
      enAssists2: events.data
        .filter(isGoal)
        .filter(isAssist2(player))
        .filter(isENGoal).length,
      goalsFor: events.data.filter(isGoalFor(player)).length,
      goalsForPP1: events.data.filter(isGoalFor(player)).filter(isPP1Goal)
        .length,
      goalsForPP2: events.data.filter(isGoalFor(player)).filter(isPP2Goal)
        .length,
      goalsForEN: events.data.filter(isGoalFor(player)).filter(isENGoal).length,
      goalsForPS: events.data.filter(isGoalFor(player)).filter(isPSGoal).length,
      goalsAgainst: events.data.filter(isGoalAgainst(player)).length,
      goalsAgainstPP1: events.data
        .filter(isGoalAgainst(player))
        .filter(isPP1Goal).length,
      goalsAgainstPP2: events.data
        .filter(isGoalAgainst(player))
        .filter(isPP2Goal).length,
      goalsAgainstEN: events.data.filter(isGoalAgainst(player)).filter(isENGoal)
        .length,
      goalsAgainstPS: events.data.filter(isGoalAgainst(player)).filter(isPSGoal)
        .length,
    }))
  const goalies = teamIds.flatMap((teamId) => stats.data.goalies[teamId])
  return { goalies, players }
}

const getCsv = (type) => (data) => Papa.unparse(data[type])

const isFinished = (game) => game.gameStatus === 2
const isErsteLiga = (game) => game.championshipName === 'Erste Liga'

const getSvsAverage = (playerRows) => {
  const gamesPlayed = playerRows.filter((row) => row.gpi).length
  if (gamesPlayed === 0) return 0
  return (
    playerRows
      .filter((row) => row.gpi)
      .reduce((c, next) => c + next.svsPercent, 0) / gamesPlayed
  )
}

const sumNumbers = (type) => (gameJsons) => {
  const playerIds = [
    ...new Set(
      gameJsons.flatMap((gameJson) => {
        const array = gameJson[type]
        const playerIds = [...new Set(array.map((p) => p.id))]
        return playerIds
      })
    ),
  ]
  return playerIds.map((playerId) => {
    const playerRows = gameJsons.flatMap((gameJson) => {
      const array = gameJson[type]
      return array.filter((player) => player.id == playerId)
    })
    return Object.keys(playerRows[0]).reduce((cum, col) => {
      const isSummableCol =
        typeof playerRows[0][col] === 'number' &&
        col !== 'teamId' &&
        col !== 'id' &&
        col !== 'jerseyNumber' &&
        col !== 'svsPercent'
      const isAvgCol = col === 'svsPercent'
      const value = isSummableCol
        ? playerRows.reduce((c, next) => c + next[col], 0)
        : isAvgCol
        ? getSvsAverage(playerRows)
        : playerRows[0][col]
      return { ...cum, [col]: value }
    }, {})
  })
}

const getAllGamesData = async (seasonId) => {
  const calendar = await getCalendar(seasonId)
  const finishedGames = calendar.data.filter(isFinished).filter(isErsteLiga)
  const gameJsons = []
  for (const game of finishedGames) {
    const gameData = await getGameData(game.id)
    const json = getGameJson(gameData)
    gameJsons.push(json)
  }
  const players = sumNumbers('players')(gameJsons)
  const goalies = sumNumbers('goalies')(gameJsons)
  return { players, goalies }
}

functions.http('get-csv', async (req, res) => {
  try {
    const { type = 'players', seasonId = 214 } = req.query || {}
    const data = await getAllGamesData(seasonId)
    const csv = getCsv(type)(data)
    // Send an HTTP response
    res.setHeader(
      'Content-disposition',
      `attachment; filename=${seasonId}-${type}.csv`
    )
    res.setHeader('Content-Type', 'text/csv')
    res.status(200).send(csv)
  } catch (error) {
    console.error(error)
    res.status(500).send(error.message)
  }
})
