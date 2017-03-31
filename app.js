var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var tedious = require('tedious');
var sql = require('mssql');
var schedule = require('node-schedule');

var index = require('./routes/index');
var users = require('./routes/users');
var api   = require('./routes/api');

var Connection = tedious.Connection;
var Request = tedious.Request;

var getConnection;

var pool;
var data = {};

var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'pug');

// uncomment after placing your favicon in /public
//app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')));
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(require('less-middleware')(path.join(__dirname, 'public')));
app.use(express.static(path.join(__dirname, 'public')));

// app.use(function(req, res, next) {
//   res.locals.data = data;
//   next();
// });

// app.use('/', index);
// app.use('/users', users);
// app.use('/api', api);

app.get('/data', function(req, res) {
  res.json(data);
});

app.get('/data/:dataKey', function(req, res) {
  res.json(data[req.params.dataKey]);
});

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

function updateBoxData() {
  new sql.Request().query("\
      SELECT \
          Count([Box ID]) as Count, \
          [Box Size] \
      FROM Access.tblBoxNumbers BN \
      WHERE \
        (BN.BoxLocation = 'Troy Avenue' \
        OR BN.BoxLocation = 'Enterprise Park')  \
        AND BN.[Box Size] IS NOT NULL \
      GROUP BY BN.[Box Size]",
    function(err, results) {
      if (err) {
        console.log(err);
      }

      data.boxes = results.recordset.reduce(function(pv, cv, i, array) {
        pv[cv['Box Size']] = cv.Count;
        return pv;
      }, {});
    });
}

function updateJobData() {
  new sql.Request().query("\
      SELECT COUNT(*) AS PendingOrderCount, IIF(P.TripType IN ('Pull', 'Roundtrip', 'Respot'), P.TripType, S.TripType) AS AppTripType \
      FROM tblDispatchSet S \
      LEFT JOIN tblDispatchPull P ON S.WorkOrderID = P.WorkOrderID \
      WHERE S.Status IN ('Comet', 'Created', 'Pending Assignment', 'Assigned') \
      GROUP BY IIF(P.TripType IN ('Pull', 'Roundtrip', 'Respot'), P.TripType, S.TripType)", 
    function(err, results) {
      if (err) {
        console.log(err);
      }

      data.jobs = results.recordset.reduce(function(pv, cv, i, array) {
        pv[cv.AppTripType] = cv.PendingOrderCount;
        return pv;
      }, {});
    });
}

function updateOntimeData() {
  new sql.Request().query("\
    SELECT \
			AVG(s.OnTime)*100 AS OnTime \
		FROM ( \
			SELECT \
				'Pull' AS ActionType, \
				DriP.ServiceDate AS 'ServiceDate', \
				CASE WHEN DATEDIFF(day, DisP.NeedBy, DriP.ServiceDate) < 1 THEN 1.0 ELSE 0.0 END AS OnTime \
			FROM tblDispatchPull DisP \
				INNER JOIN tblDriverPull DriP ON DisP.WorkOrderID = DriP.WorkOrderID \
			UNION ALL \
			SELECT \
				'Set'  AS ActionType, \
				DriS.ServiceDate AS 'ServiceDate', \
				CASE WHEN DATEDIFF(day, DisS.NeedBy, DriS.ServiceDate) < 1 THEN 1.0 ELSE 0.0 END AS OnTime \
			FROM tblDispatchSet DisS \
				INNER JOIN tblDriverSet DriS ON DisS.WorkOrderID = DriS.WorkOrderID \
		) s \
		WHERE DATEDIFF(day, s.ServiceDate, CURRENT_TIMESTAMP) < 14 \
  ", function(err, results) {
    if (err) {
      console.log(err);
    }

    data.ontime = results.recordset[0].OnTime;
  });
}

function updateOntimeHistoryData() {
  new sql.Request().query("\
      SELECT \
        MIN(s.ServiceDate) AS ServiceDate, \
        AVG(s.OnTime)*100 AS OnTime, \
        COUNT(*) AS NumberOfOrders \
      FROM ( \
        SELECT \
          'Pull' AS ActionType, \
          DriP.ServiceDate AS 'ServiceDate', \
          CASE WHEN DATEDIFF(day, DisP.NeedBy, DriP.ServiceDate) < 1 THEN 1.0 ELSE 0.0 END AS OnTime \
        FROM tblDispatchPull DisP \
          INNER JOIN tblDriverPull DriP ON DisP.WorkOrderID = DriP.WorkOrderID \
        UNION ALL \
        SELECT \
          'Set'  AS ActionType, \
          DriS.ServiceDate AS 'ServiceDate', \
          CASE WHEN DATEDIFF(day, DisS.NeedBy, DriS.ServiceDate) < 1 THEN 1.0 ELSE 0.0 END AS OnTime \
        FROM tblDispatchSet DisS \
          INNER JOIN tblDriverSet DriS ON DisS.WorkOrderID = DriS.WorkOrderID \
      ) s \
      WHERE DATEDIFF(week, s.ServiceDate, CURRENT_TIMESTAMP) < 53 \
      GROUP BY DATEPART(year, s.ServiceDate), DATEPART(week, s.ServiceDate) \
      ORDER BY ServiceDate ASC",
    function(err, results) {
      if (err) {
        console.log(err);
      }

      data.year = results.recordset.reduce(function(pv, cv, i, a) {
        pv[cv.ServiceDate] = { date: cv.ServiceDate, orders: cv.NumberOfOrders, percent: cv.OnTime };
        return pv;
      }, {});
    });
}

function update10Minute() {
  updateBoxData();
  updateJobData();
  console.log("10 minutes");

  setTimeout(update10Minute, 600000);
}

function updateDaily() {
  updateOntimeData();
  console.log("daily");

  setTimeout(updateDaily, 86400000);
}

function updateWeekly() {
  updateOntimeHistoryData();
  console.log("weekly");

  setTimeout(updateWeekly, 604800000);
}

module.exports = function(config) {

  sql.connect(config, function (err) {
    if (err) {
      console.log(err);
      return;
    }

    update10Minute();
    updateDaily();
    updateWeekly();
  });

  sql.on('error', function(err) {
    console.log(err);
  })

  return app;
};
